require 'uri'
require 'cgi'
require 'net/http'
require 'net/https'
require 'json'
require 'cause'

module InfluxDB
  class Client
    attr_accessor :hosts,
                  :port,
                  :username,
                  :password,
                  :database,
                  :precision,
                  :auth_method,
                  :use_ssl,
                  :verify_ssl,
                  :ssl_ca_cert,
                  :stopped

    attr_accessor :queue, :worker, :udp_client

    include InfluxDB::Logging

    # Initializes a new InfluxDB client
    #
    # === Examples:
    #
    #     InfluxDB::Client.new                               # connect to localhost using root/root
    #                                                        # as the credentials and doesn't connect to a db
    #
    #     InfluxDB::Client.new 'db'                          # connect to localhost using root/root
    #                                                        # as the credentials and 'db' as the db name
    #
    #     InfluxDB::Client.new :username => 'username'       # override username, other defaults remain unchanged
    #
    #     Influxdb::Client.new 'db', :username => 'username' # override username, use 'db' as the db name
    #
    #
    # === Valid options in hash
    #
    # +:host+:: the hostname to connect to
    # +:port+:: the port to connect to
    # +:username+:: the username to use when executing commands
    # +:password+:: the password associated with the username
    # +:use_ssl+:: use ssl to connect?
    # +:verify_ssl+:: verify ssl server certificate?
    # +:ssl_ca_cert+:: ssl CA certificate, chainfile or CA path. The system CA path is automatically included.
    def initialize *args
      @database = args.first if args.first.is_a? String
      opts = args.last.is_a?(Hash) ? args.last : {}
      @hosts = Array(opts[:hosts] || opts[:host] || ["localhost"])
      @port = opts[:port] || 8086
      @username = opts[:username] || "root"
      @password = opts[:password] || "root"
      @auth_method = %w{params basic_auth}.include?(opts[:auth_method]) ? opts[:auth_method] : "params"
      @use_ssl = opts[:use_ssl] || false
      @verify_ssl = opts.fetch(:verify_ssl, true)
      @ssl_ca_cert = opts[:ssl_ca_cert] || false
      @precision = opts[:precision] || "s"
      @initial_delay = opts[:initial_delay] || 0.01
      @max_delay = opts[:max_delay] || 30
      @open_timeout = opts[:write_timeout] || 5
      @read_timeout = opts[:read_timeout] || 300
      @async = opts[:async] || false
      @retry = opts.fetch(:retry, nil)
      @retry = case @retry
      when Integer
        @retry
      when true, nil
        -1
      when false
        0
      end

      @worker = InfluxDB::Worker.new(self) if @async
      self.udp_client = opts[:udp] ? InfluxDB::UDPClient.new(opts[:udp][:host], opts[:udp][:port]) : nil

      at_exit { stop! } if @retry > 0
    end

    def ping
      get "/ping"
    end

    ## allow options, e.g. influxdb.create_database('foo')
    def create_database(name)
      query "CREATE DATABASE #{name}"
    end

    def delete_database(name)
      query "DROP DATABASE #{name}"
    end

    def get_database_list
      query 'SHOW DATABASES'
    end

    def create_cluster_admin(username, password)
      query "CREATE USER #{username} WITH PASSWORD '#{password}' WITH ALL PRIVILEGES"
    end

    def get_cluster_admin_list
      query "SHOW USERS"
    end

    def create_user(username, password)
      query "CREATE USER #{username} WITH PASSWORD '#{password}'"
    end

    def delete_user(username)
      query "DROP USER #{username}"
    end

    def get_user_list
      query "SHOW USERS"
    end

    def write_point(measurement, tags, values, async=@async, precision=@precision)
      write_points([{:measurement => measurement, tags: tags, values: values}], async, precision)
    end

    # Example:
    # db.write_points(
    #     [
    #         {
    #             measurement: 'cpu_load_short',
    #             tags: {
    #                 host: 'server01',
    #                 region: 'us-west'
    #             }
    #             values: {
    #                 value1: 'val1'
    #                 value2: 'val2'
    #             }
    #         }
    #     ]
    # )
    def write_points(name_data_hashes_array, async=@async, precision=@precision)

      payloads = ""
      name_data_hashes_array.each do |attrs|
        payloads << generate_payload(attrs[:measurement], attrs[:tags], attrs[:values])
      end

      if async
        worker.push(payloads)
      elsif udp_client
        udp_client.send(payloads)
      else
        _write(payloads, precision)
      end
    end

    def generate_payload(name, tags, values)
      tags = {} unless tags
      columns = Hash[tags.sort].reduce("") { |memo, (k, v)| "#{memo},#{k}=#{v}" }
      payload = "#{name}#{columns} "

      values.each do |k, v|
        payload << "#{k}=#{v}"
      end

      payload
    end

    def _write(payload, precision=@precision)
      post full_url("/write", :precision => precision), payload
    end

    def query(query, database=@database)
      get full_url("/query", :q => query, :database => database)
    end

    def stop!
      @stopped = true
    end

    def stopped?
      @stopped
    end

    private

    def full_url(path, params={})
      unless basic_auth?
        params[:u] = @username
        params[:p] = @password
      end

      query = params.map { |k, v| [CGI.escape(k.to_s), "=", CGI.escape(v.to_s)].join }.join("&")

      URI::Generic.build(:path => path, :query => query).to_s
    end

    def basic_auth?
      @auth_method == 'basic_auth'
    end

    def get(url, return_response = false)
      connect_with_retry do |http|
        request = Net::HTTP::Get.new(url)
        request.basic_auth @username, @password if basic_auth?
        response = http.request(request)
        if response.kind_of? Net::HTTPSuccess
          if return_response
            return response
          else
            return response.body ? JSON.parse(response.body) : true
          end
        elsif response.kind_of? Net::HTTPUnauthorized
          raise InfluxDB::AuthenticationError.new response.body
        elsif response.kind_of? Net::HTTPBadRequest
          raise InfluxDB::BadRequestError.new response.body
        else
          raise InfluxDB::Error.new response.body
        end
      end
    end

    def post(url, data)
      headers = {"Content-Type" => "application/json"}
      connect_with_retry do |http|
        request = Net::HTTP::Post.new(url, headers)
        request.basic_auth @username, @password if basic_auth?
        response = http.request(request, data)
        if response.kind_of? Net::HTTPSuccess
          return response
        elsif response.kind_of? Net::HTTPUnauthorized
          raise InfluxDB::AuthenticationError.new response.body
        else
          raise InfluxDB::Error.new response.body
        end
      end
    end

    def connect_with_retry(&block)
      hosts = @hosts.dup
      delay = @initial_delay
      retry_count = 0

      begin
        hosts.push(host = hosts.shift)
        http = Net::HTTP.new(host, @port)
        http.open_timeout = @open_timeout
        http.read_timeout = @read_timeout
        http.use_ssl = @use_ssl
        http.verify_mode = OpenSSL::SSL::VERIFY_NONE unless @verify_ssl

        if @use_ssl
          store = OpenSSL::X509::Store.new
          store.set_default_paths
          if @ssl_ca_cert
            if File.directory?(@ssl_ca_cert)
              store.add_path(@ssl_ca_cert)
            else
              store.add_file(@ssl_ca_cert)
            end
          end
          http.cert_store = store
        end

        block.call(http)

      rescue Timeout::Error, *InfluxDB::NET_HTTP_EXCEPTIONS => e
        retry_count += 1
        if (@retry == -1 or retry_count <= @retry) and !stopped?
          log :error, "Failed to contact host #{host}: #{e.inspect} - retrying in #{delay}s."
          log :info, "Queue size is #{@queue.length}." unless @queue.nil?
          sleep delay
          delay = [@max_delay, delay * 2].min
          retry
        else
          raise InfluxDB::ConnectionError, "Tried #{retry_count-1} times to reconnect but failed."
        end
      ensure
        http.finish if http.started?
      end
    end

    def denormalize_series series
      columns = series['columns']

      h = Hash.new(-1)
      columns = columns.map {|v| h[v] += 1; h[v] > 0 ? "#{v}~#{h[v]}" : v }

      series['points'].map do |point|
        decoded_point = point.map do |value|
          InfluxDB::PointValue.new(value).load
        end
        Hash[columns.zip(decoded_point)]
      end
    end

    WORKER_MUTEX = Mutex.new
    def worker
      return @worker if @worker
      WORKER_MUTEX.synchronize do
        #this return is necessary because the previous mutex holder might have already assigned the @worker
        return @worker if @worker
        @worker = InfluxDB::Worker.new(self)
      end
    end
  end
end
