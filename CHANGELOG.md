# V1.0.4 (2021-03-11)

- Upgrade go version in Dockerfile [#23](https://github.com/speee/go-athena/pull/23)
- Upgrade to aws-sdk-go-v2 [#25](https://github.com/speee/go-athena/pull/25)

# V1.0.3 (2021-10-15)

- Add Prepared Statement [#20](https://github.com/speee/go-athena/pull/20)
- Upgrade packages [#20](https://github.com/speee/go-athena/pull/20)

# V1.0.2 (2021-07-03)

- Add Getting output_location from workgroup [#18](https://github.com/speee/go-athena/pull/18)
- Fix the error which occurs when the last character of output_location is '/' in GZIP DL Mode [#18](https://github.com/speee/go-athena/pull/18)

# V1.0.1 (2021-03-23)

Fixed bugs:

- Fix not skipping headers if the result contains no rows [#16](https://github.com/speee/go-athena/pull/16)

# V1.0.0 (2020-12-07)

Initial release of go-athena by speee

This is forked from [another project](https://github.com/segmentio/go-athena), which has not been maintained actively, and we started maintaining it instead.
The following functions have been added, including importing some of the issues that were originally up.

- Add Docker environment
- Add linter by reviewdog
- Remove Result Header when executing DDL
- Use workgroup
- Modify test (make each package up to date)
- Establish Result Mode (query result acquisition mode)
