# Publish all
[group('publish')]
publish_all: publish_ic_dbms_macros publish_ic_dbms_types publish_ic_dbms_canister

[group('publish')]
publish_ic_dbms_canister:
  cargo publish -p ic-dbms-canister

[group('publish')]
publish_ic_dbms_macros:
  cargo publish -p ic-dbms-macros

[group('publish')]
publish_ic_dbms_types:
  cargo publish -p ic-dbms-types