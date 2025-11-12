# Run all unit tests and integration tests
[group('test')]
test_all: test integration_test

[group('test')]
test test_name="":
  cargo test {{test_name}}

[group('test')]
integration_test test_name="":
  echo "Running integration tests..."
  cargo test {{test_name}} -p integration-tests --features "pocket-ic"
