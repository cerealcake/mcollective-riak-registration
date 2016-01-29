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

        @node = @config.pluginconf["registration.riak_node"] || "localhost"
        @bucket_type = @config.pluginconf["registration.riak_bucket_type"] || "mcollective"
        @bucket = @config.pluginconf["registration.riak_bucket"] || "node"
        @port = @config.pluginconf["registration.riak_port"] || "8087"

        @user = @config.pluginconf["registration.riak_user"] || "zedo"
        @password = @config.pluginconf["registration.riak_password"] || 'catnip'

        # The default values for the certificates are useful if your nodes are provisioned via Puppet
        # On the Riak cluster nodes, the following ca files would correspond to
        # ssl.certfile = /var/lib/puppet/ssl/certs/fqdn.pem
        # ssl.keyfile = /var/lib/puppet/ssl/private_keys/fqdn.pem
        # ssl.cacertfile = /var/lib/puppet/ssl/certs/ca.pem
        @ca_file = @config.pluginconf["registration.riak_ca_file"] || '/var/lib/puppet/ssl/certs/ca.pem'
        @client_ca = @config.pluginconf["registration.riak_client_ca"] ||  '/var/lib/puppet/ssl/certs/ca.pem'
        @cert = @config.pluginconf["registration.riak_cert"] || '/var/lib/puppet/ssl/certs/fqdn.pem'
        @key = @config.pluginconf["registration.riak_key"] || '/var/lib/puppet/ssl/private_keys/fqdn.pem'

        Log.instance.debug("Connecting to #{@node} with bucket #{@bucket}")

        @client = Riak::Client.new( 
          :authentication => {
             user: @user,
             password: @password,
             ca_file: @ca_file,
             client_ca:  @client_ca,
             cert: (File.read @cert),
             key: OpenSSL::PKey::RSA.new(File.read @key),
          },
          :host => @node, 
          :pb_port => @port
        )

        # requires 'search = on' to be set in riak.conf on the riak cluster nodes in order to work
        @client.create_search_index("facts")

        # requires bucket_type to be created already in the riak cluster nodes in order to work
        # e.g. 
        # riak-admin bucket-type create mcollective  '{"props": {"dvv_enabled": false, "last_write_wins":true}}'
        # riak-admin bucket-type activate mcollective
        @bucket_type = @client.bucket_type "#{@bucket_type}"
        @bucket = @bucket_type.bucket "#{@bucket}"

        @client.set_bucket_props @bucket, {search_index: "facts", dvv_enabled: false,last_write_wins: true}, "#{@bucket_type}"

      rescue Exception => e
        Log.instance.error("Failed to connect to riak: #{e}")
      end

      def handlemsg(msg, connection)
        data = msg[:body]
        
        if (msg.kind_of?(Array))
          Log.instance.warn("Got no facts - did you forget to add 'registration = Meta' to your server.cfg?");
          return nil
        end
        object = @bucket.get_or_new("#{data[:identity]}")
        object.data = data
        object.store type: "#{@bucket_type}"

        Log.instance.debug("node #{data[:identity]} stored in #{@riak_bucket} on riak node #{@riak_node}");

        nil
      end

      def help
      end
    end
  end
end

# vi:tabstop=2:expandtab:ai:filetype=ruby

