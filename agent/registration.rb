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
        @riak_bucket = @config.pluginconf["registration.riak_bucket"] || "mco"

        Log.instance.debug("Connecting to #{@riak_node} with bucket #{@riak_bucket}")
        @client = Riak::Client.new(:host => "#{@riak_node}")
        @bucket = @client.bucket("#{@riak_bucket}")

      end

      def handlemsg(msg, connection)
        data = msg[:body]

        if (msg.kind_of?(Array))
          Log.instance.warn("Got no facts - did you forget to add 'registration = Meta' to your server.cfg?");
          return nill
        end

        data[:lastseen] = Time.now

        object = @bucket.get_or_new("#{data[:identity]}")
        object.data = data
        object.store

        Log.instance.debug("node #{data[:identity]} stored in #{@riak_bucket} on riak node #{@riak_node}");

        nil
      end

      def help
      end
    end
  end
end

# vi:tabstop=2:expandtab:ai:filetype=ruby

