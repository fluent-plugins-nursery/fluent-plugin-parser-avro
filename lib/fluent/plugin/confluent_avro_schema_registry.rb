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

require "net/http"
require "uri"

module Fluent
  module Plugin
    class ConfluentAvroSchemaRegistry
      def initialize(registry_url)
        @registry_url = registry_url
      end

      def subject_version(subject, schema_key, version = "latest")
        registry_uri = URI.parse(@registry_url)
        registry_uri_with_versions = URI.join(registry_uri, "/subjects/#{subject}/versions/#{version}")
        response = Net::HTTP.get_response(registry_uri_with_versions)
        if schema_key.nil?
          response.body
        else
          Yajl.load(response.body)[schema_key]
        end
      end

      def schema_with_id(schema_id, schema_key)
        registry_uri = URI.parse(@registry_url)
        registry_uri_with_ids = URI.join(registry_uri, "/schemas/ids/#{schema_id}")
        response = Net::HTTP.get_response(registry_uri_with_ids)
        if schema_key.nil?
          response.body
        else
          Yajl.load(response.body)[schema_key]
        end
      end
    end
  end
end
