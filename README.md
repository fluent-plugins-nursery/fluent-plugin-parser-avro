# fluent-plugin-parser-avro

![Testing on Ubuntu](https://github.com/fluent-plugins-nursery/fluent-plugin-parser-avro/workflows/Testing%20on%20Ubuntu/badge.svg?branch=master)
![Testing on Windows](https://github.com/fluent-plugins-nursery/fluent-plugin-parser-avro/workflows/Testing%20on%20Windows/badge.svg?branch=master)

[Fluentd](https://fluentd.org/) parser plugin to parse avro formatted data.

## Installation

### RubyGems

```
$ gem install fluent-plugin-avro
```

### Bundler

Add following line to your Gemfile:

```ruby
gem "fluent-plugin-avro"
```

And then execute:

```
$ bundle
```

## Configuration

* **schema_file** (string) (optional): avro schema file path.
* **schema_json** (string) (optional): avro schema definition hash.
* **schema_url** (string) (optional): avro schema remote URL.
* **schema_registery_with_subject_url** (string) (optional): avro schema registry URL.
* **schema_url_key** (string) (optional): avro schema registry or something's response schema key.
* **writers_schema_file** (string) (optional): avro schema file path for writers definition.
* **writers_schema_json** (string) (optional): avro schema definition hash for writers definition.
* **readers_schema_file** (string) (optional): avro schema file path for readers definition.
* **readers_schema_json** (string) (optional): avro schema definition hash for readers definition.

### Configuration Example

```aconf
<parse>
  @type avro
  # schema_file /path/to/file
  # schema_json { "namespace": "org.fluentd.parser.avro", "type": "record", "name": "User", "fields" : [{"name": "username", "type": "string"}, {"name": "age", "type": "int"}, {"name": "verified", "type": ["boolean", "null"], "default": false}]}
  # schema_url http(s)://[server fqdn]:[port]/subjects/[a great user's subject]/[the latest schema version]
  # schema_key schema
  # schema_registery_with_subject_url http(s)://[server fqdn]:[port]/subjects/[a great user's subject]/
</parse>
```

## AVRO schema registry support

Confluent AVRO schema registry should respond with REST API.

This plugin uses the following API:

* [`GET /subjects/(string: subject)/versions`](https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions)
* [`GET /subjects/(string: subject)/versions/(versionId: version)`](https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions)

Users can specify a URL for retrieving the latest schemna information:

e.g.) `http(s)://[server fqdn]:[port]/subjects/[a great user's subject]/`

For example, when specifying the following configuration:

```
<parse>
  @type avro
  schema_registery_with_subject_url http://localhost:8081/subjects/persons-avro-value/
```

Then the parser plugin calls `GET http://localhost:8081/subjects/persons-avro-value/versions/` to retrive the registered schema versions and then calls `GET GET http://localhost:8081/subjects/persons-avro-value/versions/<the latest schema version>`.

## Copyright

* Copyright(c) 2020- Hiroshi Hatake
* License
  * Apache License, Version 2.0
