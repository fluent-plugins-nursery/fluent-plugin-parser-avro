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
require "stringio"
require "fluent/plugin/parser"

module Fluent
  module Plugin
    class AvroParser < Fluent::Plugin::Parser
      Fluent::Plugin.register_parser("avro", self)

      config_param :schema_file, :string, :default => nil
      config_param :schema_json, :string, :default => nil
      config_param :writers_schema_file, :string, :default => nil
      config_param :writers_schema_json, :string, :default => nil
      config_param :readers_schema_file, :string, :default => nil
      config_param :readers_schema_json, :string, :default => nil

      def configure(conf)
        super

        if (!@writers_schema_file.nil? || !@writers_schema_json.nil?) &&
           (!@readers_schema_file.nil? || !@readers_schema_json.nil?)
          if !((@writers_schema_json.nil? ? 0 : 1) + (@writers_schema_file.nil? ? 0 : 1) == 1)
            raise Fluent::ConfigError, "writers_schema_json, writers_schema_file is required, but they cannot specify at the same time!"
          end
          if !((@readers_schema_json.nil? ? 0 : 1) + (@readers_schema_file.nil? ? 0 : 1) == 1)
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
          if !((@schema_json.nil? ? 0 : 1) + (@schema_file.nil? ? 0 : 1) == 1)
            raise Fluent::ConfigError, "schema_json, schema_file is required, but they cannot specify at the same time!"
          end

          @raw_schema = if @schema_file
                        File.read(@schema_file)
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
          raise e
        end
      end
    end
  end
end
