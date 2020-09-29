#
# Copyright 2020- Hiroshi Hatake
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require "avro"
require "net/http"
require "stringio"
require "uri"
require "fluent/plugin/parser"

module Fluent
  module Plugin
    class AvroParser < Fluent::Plugin::Parser
      Fluent::Plugin.register_parser("avro", self)

      config_param :schema_file, :string, :default => nil
      config_param :schema_json, :string, :default => nil
      config_param :schema_url, :string, :default => nil
      config_param :schema_registery_with_subject_url, :string, :default => nil
      config_param :schema_url_key, :string, :default => nil
      config_param :writers_schema_file, :string, :default => nil
      config_param :writers_schema_json, :string, :default => nil
      config_param :readers_schema_file, :string, :default => nil
      config_param :readers_schema_json, :string, :default => nil

      def configure(conf)
        super

        if (!@writers_schema_file.nil? || !@writers_schema_json.nil?) &&
           (!@readers_schema_file.nil? || !@readers_schema_json.nil?)
          unless [@writers_schema_json, @writers_schema_file].compact.size == 1
            raise Fluent::ConfigError, "writers_schema_json, writers_schema_file is required, but they cannot specify at the same time!"
          end
          unless [@readers_schema_json, @readers_schema_file].compact.size == 1
            raise Fluent::ConfigError, "readers_schema_json, readers_schema_file is required, but they cannot specify at the same time!"
          end

          @writers_raw_schema = if @writers_schema_file
                                  File.read(@writers_schema_file)
                                elsif @writers_schema_json
                                  @writers_schema_json
                                end
          @readers_raw_schema = if @readers_schema_file
                                  File.read(@readers_schema_file)
                                elsif @readers_schema_json
                                  @readers_schema_json
                                end

          @writers_schema = Avro::Schema.parse(@writers_raw_schema)
          @readers_schema = Avro::Schema.parse(@readers_raw_schema)
          @reader = Avro::IO::DatumReader.new(@writers_schema, @readers_schema)
        else
          unless [@schema_json, @schema_file, @schema_url, @schema_registery_with_subject_url].compact.size == 1
            raise Fluent::ConfigError, "schema_json, schema_file, or schema_url is required, but they cannot specify at the same time!"
          end
          if @schema_registery_with_subject_url && !@schema_registery_with_subject_url.end_with?("/")
            raise Fluent::ConfigError, "schema_registery_with_subject_url must contain the trailing slash('/')."
          end

          @raw_schema = if @schema_file
                          File.read(@schema_file)
                        elsif @schema_registery_with_subject_url
                          fetch_latest_schema(@schema_registery_with_subject_url, @schema_url_key)
                        elsif @schema_url
                          fetch_schema(@schema_url, @schema_url_key)
                        elsif @schema_json
                          @schema_json
                        end

          @schema = Avro::Schema.parse(@raw_schema)
          @reader = Avro::IO::DatumReader.new(@schema)
        end
      end

      def parser_type
        :binary
      end

      def parse(data)
        buffer = StringIO.new(data)
        decoder = Avro::IO::BinaryDecoder.new(buffer)
        begin
          decoded_data = @reader.read(decoder)
          time, record = convert_values(parse_time(decoded_data), decoded_data)
          yield time, record
        rescue => e
          raise e if @schema_url.nil? or @schema_registery_with_subject_url.nil?
          begin
            new_raw_schema = if @schema_url
                               fetch_schema(@schema_url, @schema_url_key)
                             elsif @schema_registery_with_subject_url
                               fetch_latest_schema(@schema_registery_with_subject_url, @schema_url_key)
                             end
            new_schema = Avro::Schema.parse(new_raw_schema)
            is_changed = (new_raw_schena == @raw_schema)
            @raw_schema = new_raw_schema
            @schame = new_schema
          rescue
            # Do nothing.
          end
          if is_changed
            decoded_data = @reader.read(decoder)
            time, record = convert_values(parse_time(decoded_data), decoded_data)
            yield time, record
          else
            raise e
          end
        end
      end

      def fetch_schema_versions(base_uri_with_versions)
        versions_response = Net::HTTP.get_response(base_uri_with_versions)
        Yajl.load(versions_response.body)
      end

      def fetch_latest_schema(base_url, schema_key)
        base_uri = URI.parse(base_url)
        base_uri_with_versions = URI.join(base_uri, "versions/")
        versions = fetch_schema_versions(base_uri_with_versions)
        uri = URI.join(base_uri_with_versions, versions.last.to_s)
        response = Net::HTTP.get_response(uri)
        if schema_key.nil?
          response.body
        else
          Yajl.load(response.body)[schema_key]
        end
      end

      def fetch_schema(url, schema_key)
        uri = URI.parse(url)
        response = Net::HTTP.get_response(uri)
        if schema_key.nil?
          response.body
        else
          Yajl.load(response.body)[schema_key]
        end
      end
    end
  end
end
