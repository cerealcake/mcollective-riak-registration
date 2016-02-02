module MCollective
  module Agent
    class Registration<RPC::Agent
      attr_reader :timeout, :meta

      def initialize
        @meta = {:license => "Apache 2",
          :author => "Jaime Viloria <jaime.viloria@gmail.com>",
          :url => "https://github.com/cerealcake/mcollective-riak-registration"}

        require 'riak'

        @config = Config.instance

        @riak_node = @config.pluginconf["registration.riak_node"] || "localhost"
        @search_schema = @config.pluginconf["registration.riak_schema"] || "/usr/libexec/mcollective/mcollective/agent/registration.xml"
        @bck_type_name = @config.pluginconf["registration.riak_bucket_type"] || "mcollective"
        @bucket_name = @config.pluginconf["registration.riak_bucket"] || "nodes"
        @pb_port = @config.pluginconf["registration.riak_pb_port"] || "8087"

        @user = @config.pluginconf["registration.riak_user"] || "foo"
        @password = @config.pluginconf["registration.riak_password"] || 'bar'

        # The default values for the certificates are useful if your nodes are provisioned via Puppet
        # On the Riak cluster nodes, the following ca files would correspond to
        # ssl.certfile = /var/lib/puppet/ssl/certs/fqdn.pem
        # ssl.keyfile = /var/lib/puppet/ssl/private_keys/fqdn.pem
        # ssl.cacertfile = /var/lib/puppet/ssl/certs/ca.pem
        @ca_file = @config.pluginconf["registration.riak_ca_file"] || '/var/lib/puppet/ssl/certs/ca.pem'
        @client_ca = @config.pluginconf["registration.riak_client_ca"] ||  '/var/lib/puppet/ssl/certs/ca.pem'
        @cert = @config.pluginconf["registration.riak_cert"] || '/var/lib/puppet/ssl/certs/fqdn.pem'
        @key = @config.pluginconf["registration.riak_key"] || '/var/lib/puppet/ssl/private_keys/fqdn.pem'

        Log.instance.debug("Connecting to #{@riak_node} with bucket #{@bucket_name}")

        @client = Riak::Client.new( 
          :authentication => {
             user: @user,
             password: @password,
             ca_file: @ca_file,
             client_ca:  @client_ca,
             cert: (File.read @cert),
             key: OpenSSL::PKey::RSA.new(File.read @key),
          },
          :host => @riak_node, 
          :pb_port => @pb_port
         }

        # requires bucket_type to be created already in the riak cluster nodes in order to work
        # e.g. 
        # riak-admin bucket-type create mcollective  '{"props": {"dvv_enabled": false, "last_write_wins":true}}'
        # riak-admin bucket-type activate mcollective
        @bck_type = @client.bucket_type "#{@bck_type_name}"

        # the current schema above using registration.xml allows for queries like the following
        # curl 'https://localhost:8098/search/query/nodes?wt=json&q=identity:foo.bar.now'
        # curl 'https://localhost:8098/search/query/nodes?wt=json&q=facts.processors.count:2'
        @schema_data = File.read(@search_schema)
        @client.create_search_schema("registration", @schema_data)

        @nodes = @bck_type.bucket "#{@bucket_name}"
        
        @client.create_search_index("nodes","registration")
        @client.set_bucket_props @nodes, {search_index: "nodes", dvv_enabled: false,last_write_wins: true}, "#{@bck_type_name}"

      end

      def handlemsg(msg, connection)
        data = msg[:body]
        data[:lastseen]=DateTime.now
        data[:epoch]=Time.now.to_i
        
        if (msg.kind_of?(Array))
          Log.instance.warn("Got no facts - did you forget to add 'registration = Meta' to your server.cfg?");
          return nil
        end

        
        object = @nodes.get_or_new("#{data[:identity]}")
        object.data = data
        object.store type: "#{@bck_type_name}"

        Log.instance.debug("node #{data[:identity]} stored in #{@bucket_name} on riak node #{@riak_node}");

        nil
      rescue Exception => e
        Log.instance.error("Failed to update to riak db: #{e}")
      end

      def help
      end
    end
  end
end

