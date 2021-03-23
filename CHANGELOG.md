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

# V1.0.1 (2021-03-23)

Fixed bugs:

- Fix not skipping headers if the result contains no rows [#16](https://github.com/speee/go-athena/pull/16)
