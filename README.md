# Amazon Athena Query Federation Javadoc

[![Build Status](https://github.com/awslabs/aws-athena-query-federation/workflows/Java%20CI%20Push/badge.svg)](https://github.com/awslabs/aws-athena-query-federation/actions)

This branch (gh-pages) is intended only for javadoc for the aws-athena-query-federation project and should reflect the source in the master branch. To manually sync:

Create a fork of awslabs/aws-athena-query-federation

```
git checkout master
mvn javadoc:aggregate
git checkout gh-pages
cp -r target/site/apidocs/* .
git commit -am "syncing javadoc from master"
git push
```

Then open a pull request against awslabs/aws-athena-query-federation/gh-pages.

## License

This project is licensed under the Apache-2.0 License.
