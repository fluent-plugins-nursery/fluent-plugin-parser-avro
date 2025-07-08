lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name    = "fluent-plugin-parser-avro"
  spec.version = "0.3.1"
  spec.authors = ["Hiroshi Hatake", "Kentaro Hayashi"]
  spec.email   = ["cosmo0920.wp@gmail.com", "kenhys@gmail.com"]

  spec.summary       = %q{Avro parser plugin for Fluentd}
  spec.description   = spec.summary
  spec.homepage      = "https://github.com/fluent-plugins-nursery/fluent-plugin-parser-avro"
  spec.license       = "Apache-2.0"

  test_files, files  = `git ls-files -z`.split("\x0").partition do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.files         = files
  spec.executables   = files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = test_files
  spec.require_paths = ["lib"]

  spec.add_dependency "avro"
  spec.add_development_dependency "bundler", "~> 2.1"
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "test-unit", "~> 3.3"
  spec.add_development_dependency "webrick"
  spec.add_runtime_dependency "fluentd", [">= 0.14.10", "< 2"]
end
