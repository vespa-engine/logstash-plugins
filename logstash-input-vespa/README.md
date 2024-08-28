# Logstash Input Plugin for Vespa

Plugin for [Logstash](https://github.com/elastic/logstash) to read from [Vespa](https://vespa.ai). Apache 2.0 license.

## Installation

Download and unpack/install Logstash, then:
```
bin/logstash-plugin install logstash-input-vespa
```

## Usage

Minimal Logstash config example:
```
input {
  vespa {
    vespa_url => "http://localhost:8080"
    cluster => "test_cluster"
  }
}

output {
  stdout {}
}
```