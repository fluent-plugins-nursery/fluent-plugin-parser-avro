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
require_relative "./confluent_avro_schema_registry"

module Fluent
  module Plugin
    class AvroParser < Fluent::Plugin::Parser
      Fluent::Plugin.register_parser("avro", self)

      MAGIC_BYTE = [0].pack("C").freeze

      config_param :schema_file, :string, :default => nil
      config_param :schema_json, :string, :default => nil
      config_param :schema_url, :string, :default => nil
      config_param :schema_url_key, :string, :default => nil
      config_param :writers_schema_file, :string, :default => nil
      config_param :writers_schema_json, :string, :default => nil
      config_param :readers_schema_file, :string, :default => nil
      config_param :readers_schema_json, :string, :default => nil
      config_param :use_confluent_schema, :bool, :default => true
      config_param :api_key, :string, :default => nil
      config_param :api_secret, :string, :default => nil
      config_section :confluent_registry, param_name: :avro_registry, required: false, multi: false do
        config_param :url, :string
        config_param :subject, :string
        config_param :schema_key, :string, :default => "schema"
        config_param :schema_version, :string, :default => "latest"
      end

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
        elsif @avro_registry
          @confluent_registry = Fluent::Plugin::ConfluentAvroSchemaRegistry.new(@avro_registry.url, @api_key, @api_secret)
          @raw_schema = @confluent_registry.subject_version(@avro_registry.subject,
                                                            @avro_registry.schema_key,
                                                            @avro_registry.schema_version)
          @schema = Avro::Schema.parse(@raw_schema)
          @reader = Avro::IO::DatumReader.new(@schema)
        else
          unless [@schema_json, @schema_file, @schema_url].compact.size == 1
            raise Fluent::ConfigError, "schema_json, schema_file, or schema_url is required, but they cannot specify at the same time!"
          end

          @raw_schema = if @schema_file
                          File.read(@schema_file)
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
          if @use_confluent_schema || @avro_registry
            # When using confluent avro schema, record is formatted as follows:
            #
            # MAGIC_BYTE | schema_id | record
            # ----------:|:---------:|:---------------
            #  1byte     |  4bytes   | record contents
            magic_byte = decoder.read(1)

            if magic_byte != MAGIC_BYTE
              raise "The first byte should be magic byte but got {magic_byte.inspect}"
            end
            schema_id = decoder.read(4).unpack("N").first
          end
          decoded_data = @reader.read(decoder)
          time, record = convert_values(parse_time(decoded_data), decoded_data)
          yield time, record
        rescue EOFError, RuntimeError => e
          raise e unless [@schema_url, @avro_registry].compact.size == 1
          begin
            new_raw_schema = if @schema_url
                               fetch_schema(@schema_url, @schema_url_key)
                             elsif @avro_registry
                               @confluent_registry.schema_with_id(schema_id,
                                                                  @avro_registry.schema_key)
                             end
            new_schema = Avro::Schema.parse(new_raw_schema)
            is_changed = (new_raw_schema != @raw_schema)
            @raw_schema = new_raw_schema
            @schema = new_schema
          rescue EOFError, RuntimeError
            # Do nothing.
          end
          if is_changed
            buffer = StringIO.new(data)
            decoder = Avro::IO::BinaryDecoder.new(buffer)
            if @use_confluent_schema || @avro_registry
              # When using confluent avro schema, record is formatted as follows:
              #
              # MAGIC_BYTE | schema_id | record
              # ----------:|:---------:|:---------------
              #  1byte     |  4bytes   | record contents
              magic_byte = decoder.read(1)

              if magic_byte != MAGIC_BYTE
                raise "The first byte should be magic byte but got {magic_byte.inspect}"
              end
              schema_id = decoder.read(4).unpack("N").first
            end
            @reader = Avro::IO::DatumReader.new(@schema)
            decoded_data = @reader.read(decoder)
            time, record = convert_values(parse_time(decoded_data), decoded_data)
            yield time, record
          else
            raise e
          end
        end
      end

      def fetch_schema(url, schema_key)
        uri = URI.parse(url)
        response = if @api_key and @api_secret
                     Net::HTTP.start(uri.host, uri.port, :use_ssl => (uri.scheme == "https")) do |http|
                       request = Net::HTTP::Get.new(uri.path)
                       request.basic_auth(@api_key, @api_secret)
                       http.request(request)
                     end
                   else
                     Net::HTTP.get_response(uri)
                   end
        if schema_key.nil?
          response.body
        else
          Yajl.load(response.body)[schema_key]
        end
      end
    end
  end
end
