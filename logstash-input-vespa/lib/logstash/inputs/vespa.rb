# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "net/http"
require "uri"
require "json"

# This is the logstash vespa input plugin. It is used to read from Vespa
# via Visit : https://docs.vespa.ai/en/reference/document-v1-api-reference.html#visit
# Each document becomes an event.

class LogStash::Inputs::Vespa < LogStash::Inputs::Base
  config_name "vespa"

  # We should get JSON from Vespa, so let's use the JSON codec.
  default :codec, "json"

  # The URL to use to connect to Vespa.
  config :vespa_url, :validate => :uri, :default => "http://localhost:8080"

  # The cluster parameter to use in the request.
  config :cluster, :validate => :string, :required => true

  public
  def register
    # nothing to do here
  end # def register

  def run(queue)
    uri = URI.parse("#{@vespa_url}/document/v1/?cluster=#{@cluster}")
    continuation = nil

    loop do

      if continuation != nil
        uri.query = URI.encode_www_form({:cluster => @cluster, :continuation => continuation})
      end
      
      # TODO we'll want to retry here
      begin
        response = Net::HTTP.get_response(uri)
      rescue => e
        @logger.error("Failed to make HTTP request to Vespa", :error => e.message)
        break
      end
      # response should look like:
      # {
      #   "pathId":"/document/v1/","documents":[
      #     {"id":"id:namespace:doctype::docid","fields":{"field1":"value1","field2":7.0}}
      #   ],
      #   "documentCount":1,"continuation":"continuation_string"
      # }

      if response.is_a?(Net::HTTPSuccess)
        begin
          response_parsed = JSON.parse(response.body)
        rescue JSON::ParserError => e
          @logger.error("Failed to parse JSON response", :error => e.message)
          break
        end

        document_count = response_parsed["documentCount"]
        # record the continuation token for the next request (if it exists)
        continuation = response_parsed["continuation"]
        documents = response_parsed["documents"]

        documents.each do |document|
          # TODO we need to also get the document ID, namespace and doctype
          event = LogStash::Event.new(document["fields"])
          decorate(event)
          queue << event
        end # documents.each

        # Exit the loop if there are no more documents to process
        break if document_count == 0

      else
        @logger.error("Failed to fetch documents from Vespa",
                      :response_code => response.code, :response_message => response.message)
        break # TODO retry? Only on certain codes?
      end # if response.is_a?(Net::HTTPSuccess)

    end # loop do
  end # def run

  def stop
    # TODO
    # examples of common "stop" tasks:
    #  * close sockets (unblocking blocking reads/accepts)
    #  * cleanup temporary files
    #  * terminate spawned threads
  end
end # class LogStash::Inputs::Vespa
