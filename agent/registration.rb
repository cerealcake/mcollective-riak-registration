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
        @riak_bucket_type = @config.pluginconf["registration.riak_bucket_type"] || "mcollective"
        @riak_bucket = @config.pluginconf["registration.riak_bucket"] || "registration"

        Log.instance.debug("Connecting to #{@riak_node} with bucket #{@riak_bucket}")

        @client = Riak::Client.new(:host => "#{@riak_node}")
        @client.create_search_index("node")

        @bucket_type = @client.bucket_type "#{@riak_bucket_type}"
        @bucket = @bucket_type.bucket "#{@riak_bucket}"

        @client.set_bucket_props @bucket, {search_index: "node", dvv_enabled: false,last_write_wins: true}, "#{@riak_bucket_type}"

      end

      def handlemsg(msg, connection)
        data = msg[:body].to_h

        if (msg.kind_of?(Array))
          Log.instance.warn("Got no facts - did you forget to add 'registration = Meta' to your server.cfg?");
          return nil
        end
        object = @bucket.get_or_new("#{data[:identity]}")
        object.data = data
        object.store type: "#{@riak_bucket_type}"

        Log.instance.debug("node #{data[:identity]} stored in #{@riak_bucket} on riak node #{@riak_node}");

        nil
      end

      def help
      end
    end
  end
end

# vi:tabstop=2:expandtab:ai:filetype=ruby

