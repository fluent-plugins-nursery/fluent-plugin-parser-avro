# fluent-plugin-avro

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
* **writers_schema_file** (string) (optional): avro schema file path for writers definition.
* **writers_schema_json** (string) (optional): avro schema definition hash for writers definition.
* **readers_schema_file** (string) (optional): avro schema file path for readers definition.
* **readers_schema_json** (string) (optional): avro schema definition hash for readers definition.

## Copyright

* Copyright(c) 2020- Hiroshi Hatake
* License
  * Apache License, Version 2.0
