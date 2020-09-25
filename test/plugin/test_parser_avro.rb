require "helper"
require "fluent/plugin/parser_avro.rb"

class AvroParserTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
  end

  SCHEMA = <<-JSON
    { "namespace": "org.fluentd.parser.avro",
      "type": "record",
      "name": "User",
      "fields" : [
        {"name": "username", "type": "string"},
        {"name": "age", "type": "int"},
        {"name": "verified", "type": ["boolean", "null"], "default": false}
    ]}
    JSON

  READERS_SCHEMA = <<-JSON
    { "namespace": "org.fluentd.parser.avro",
      "type": "record",
      "name": "User",
      "fields" : [
        {"name": "username", "type": "string"},
        {"name": "age", "type": "int"}
    ]}
    JSON

  COMPLEX_SCHEMA = <<-EOC
    {
      "type" : "record",
      "name" : "ComplexClass",
      "namespace" : "org.fluentd.parser.avro.complex.example",
      "fields" : [ {
        "name" : "time",
        "type" : "string"
      }, {
        "name" : "image",
        "type" : {
          "type" : "record",
          "name" : "image",
          "fields" : [ {
            "name" : "src",
            "type" : "string"
          }, {
            "name" : "mime_type",
            "type" : "string"
          }, {
            "name" : "height",
            "type" : "long"
          }, {
            "name" : "width",
            "type" : "long"
          }, {
            "name" : "alignment",
            "type" : "string"
          } ]
        }
      }, {
        "name" : "data",
        "type" : {
          "type" : "record",
          "name" : "data",
          "fields" : [ {
            "name" : "size",
            "type" : "long"
          }, {
            "name" : "hidden",
            "type" : "boolean"
          } ]
        }
      } ]
    }
  EOC

  def test_parse
    conf = {
      'schema_json' => SCHEMA
    }
    d = create_driver(conf)
    datum = {"username" => "foo", "age" => 42, "verified" => true}
    encoded = encode_datum(datum, SCHEMA)
    d.instance.parse(encoded) do |_time, record|
      assert_equal datum, record
    end

    datum = {"username" => "baz", "age" => 34}
    encoded = encode_datum(datum, SCHEMA)
    d.instance.parse(encoded) do |_time, record|
      assert_equal datum.merge("verified" => nil), record
    end
  end

  def test_parse_with_avro_schema
    conf = {
      'schema_file' => File.join(__dir__, "..", "data", "user.avsc")
    }
    d = create_driver(conf)
    datum = {"username" => "foo", "age" => 42, "verified" => true}
    encoded = encode_datum(datum, SCHEMA)
    d.instance.parse(encoded) do |_time, record|
      assert_equal datum, record
    end

    datum = {"username" => "baz", "age" => 34}
    encoded = encode_datum(datum, SCHEMA)
    d.instance.parse(encoded) do |_time, record|
      assert_equal datum.merge("verified" => nil), record
    end
  end

  def test_parse_with_readers_and_writers_schema
    conf = {
      'writers_schema_json' => SCHEMA,
      'readers_schema_json' => READERS_SCHEMA,
    }
    d = create_driver(conf)
    datum = {"username" => "foo", "age" => 42, "verified" => true}
    encoded = encode_datum(datum, SCHEMA)
    d.instance.parse(encoded) do |_time, record|
      datum.delete("verified")
      assert_equal datum, record
    end
  end

  def test_parse_with_readers_and_writers_schema_files
    conf = {
      'writers_schema_file' => File.join(__dir__, "..", "data", "writer_user.avsc"),
      'readers_schema_file' => File.join(__dir__, "..", "data", "reader_user.avsc"),
    }
    d = create_driver(conf)
    datum = {"username" => "foo", "age" => 42, "verified" => true}
    encoded = encode_datum(datum, SCHEMA)
    d.instance.parse(encoded) do |_time, record|
      datum.delete("verified")
      assert_equal datum, record
    end
  end

  def test_parse_with_complex_schema
    conf = {
      'schema_json' => COMPLEX_SCHEMA,
      'time_key' => 'time'
    }
    d = create_driver(conf)
    time_str = "2020-09-25 15:08:09.082113 +0900"
    datum = {
      "time" => time_str,
      "image" => {
        "src" => "images/avroexam.png",
        "mime_type"=> "image/png",
        "height" => 320,
        "width" => 280,
        "alignment" => "center"
      },
      "data" => {
        "size" => 36,
        "hidden" => false
      }
    }

    encoded = encode_datum(datum, COMPLEX_SCHEMA)
    d.instance.parse(encoded) do |time, record|
      assert_equal Time.parse(time_str).to_r, time.to_r
      datum.delete("time")
      assert_equal datum, record
    end
  end

  private

  def encode_datum(datum, string_schema)
    buffer = StringIO.new
    encoder = Avro::IO::BinaryEncoder.new(buffer)
    schema = Avro::Schema.parse(string_schema)
    writer = Avro::IO::DatumWriter.new(schema)
    writer.write(datum, encoder)
    buffer.rewind
    buffer.read
  end

  def create_driver(conf)
    Fluent::Test::Driver::Parser.new(Fluent::Plugin::AvroParser).configure(conf)
  end
end
