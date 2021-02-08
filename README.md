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
* **schema_url_key** (string) (optional): avro schema registry or something's response schema key.
* **writers_schema_file** (string) (optional): avro schema file path for writers definition.
* **writers_schema_json** (string) (optional): avro schema definition hash for writers definition.
* **readers_schema_file** (string) (optional): avro schema file path for readers definition.
* **readers_schema_json** (string) (optional): avro schema definition hash for readers definition.
* **use_confluent_schema** (bool) (optional): Assume to use confluent schema. Confluent avro schema uses the first 5-bytes for magic byte (1 byte) and schema_id (4 bytes). This parameter specifies to skip reading the first 5-bytes or not.
  * Default value: `true`.
* **api_key** (string) (optional): Set key for Basic authentication.
* **api_secret** (string) (optional): Set secret for Basic authentication.

### \<confluent_registry\> section (optional) (single)

* **url** (string) (required): confluent schema registry URL.
* **subject** (string) (required): Specify schema subject.
* **schema_key** (string) (optional): Specify schema key on confluent registry REST API response.
  * Default value: `schema`.
* **schema_version** (string) (optional): Specify schema version for the specified subject.
  * Default value: `latest`.

### Configuration Example

```aconf
<parse>
  @type avro
  # schema_file /path/to/file
  # schema_json { "namespace": "org.fluentd.parser.avro", "type": "record", "name": "User", "fields" : [{"name": "username", "type": "string"}, {"name": "age", "type": "int"}, {"name": "verified", "type": ["boolean", "null"], "default": false}]}
  # schema_url http(s)://[server fqdn]:[port]/subjects/[a great user's subject]/[the latest schema version]
  # schema_key schema
  # When using with confluent registry without <confluent_registry>, this parameter must be true.
  # use_confluent_schema true
  #<confluent_registry>
  #  url http://localhost:8081/
  #  subject your-awesome-subject
  #  # schema_key schema
  #  # schema_version 1
  #</confluent_registry>
</parse>
```

## AVRO schema registry support

Confluent AVRO schema registry should respond with REST API.

This plugin uses the following API:

* [`GET /subjects/(string: subject)/versions/(versionId: version)`](https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions)
* [`GET /schemas/ids/(int: id)`](https://docs.confluent.io/current/schema-registry/develop/api.html#get--schemas-ids-int-%20id)

Users can specify a URL for retrieving the latest schemna information with `<confluent_registry>`:

e.g.)
```
  <confluent_registry>
    url http://[confluent registry server ip]:[port]/
    subject your-awesome-subject
    # schema_key schema
    # schema_version 1
  </confluent_registry>
```

For example, when specifying the following configuration:

```
<parse>
  @type avro
  <confluent_registry>
    url http://localhost:8081/
    subject persons-avro-value
    # schema_key schema
    # schema_version 1
  </confluent_registry>
```

Then the parser plugin calls `GET http://localhost:8081/subjects/persons-avro-value/versions/latest` to retrive the registered schema versions. And when parsing failure occurred, this plugin will call `GET http://localhost:8081/schemas/ids/<schema id which is obtained from the second record on avro schema>`.

If you use this plugin to parse confluent schema, please specify `use_confluent_schema` as `true`.

This is because, confluent avro schema uses the following structure:

MAGIC_BYTE | schema_id | record
----------:|:---------:|:---------------
 1byte     |  4bytes   | record contents

When specifying `<confluent_registry>` section on configuration, this plugin will skip to read the first 5-bytes automatically and parse `schema_id` from there.

## Copyright

* Copyright(c) 2020- Hiroshi Hatake
* License
  * Apache License, Version 2.0
