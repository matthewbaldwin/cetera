# Cetera

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/2551abfcba53421898f1d6729b35021c)](https://www.codacy.com/app/engineering-github-read-only/cetera)

Cetera -- the golden-throated search service.

Basically a wrapper around elasticsearch to enable keyword search to return dataset metadata for
use by the front end in displaying search results.

By default, cetera runs on port 5704.

## API

Check out [Cetera's Apiary spec](http://docs.socratadiscovery.apiary.io/#) for the latest API info.

## Run locally
`sbt cetera-http/run`

## Setup

[Check out this great doc that Katie Vedder put together!](https://docs.google.com/document/d/1wsslEGnp15STX8UnUFZ_kLzxDHkpLWstvAa-9OS31Gk/edit)

### Dependencies

#### Java 1.8

We recommend using [jEnv](http://www.jenv.be/) to manage your java versions. jEnv is a bit like
rbenv; it uses shims to run the chosen java version for your application. jEnv will read the
.java-version file from Cetera's root directory and correctly run Java 1.8.

#### Scala 2.11.7

Scala 2.11.7 is the current (as of 2015-01-07) 2.11.x release.

Scala 2.12 targets Java 8 only, and offers a number of useful features (not the least of which is
better tab completion in the Scala REPL). We would like to upgrade to Scala 2.12 when this becomes
possible.

### Configuration

In `configs`, you'll find `sample-cetera.conf`, which contains some of the parameters you need to
run cetera locally.

You should also create `local-cetera.conf`, which is gitignored.  Configurations in this file
override those in `sample-cetera.conf`.  To use authentication, add to this file a `core` key with
`host`, `appToken`, and `port` values, like this:

``` core { host = "localhost" appToken = REDACTED port = 8081 } ```

### Elasticsearch setup

Cetera, in development use, assumes an Elasticsearch setup as follows:

- Java version: 1.8.0
- ES version: 5.2.2
- Host: localhost (127.0.0.1)
- Port: 9200 for HTTP requests and 9300 for Transport (java access)
- Cluster name: es_dev

Assuming ES was installed with Homebrew, set the cluster name to `es_dev` in
`/usr/local/etc/elasticsearch/elasticsearch.yml`

For non-homebrew installations, please find and edit this file.

If your ES cluster name is something other than `es_dev`, be sure you update the `es-cluster-name`
setting in `local-cetera.conf` accordingly.

Start Elasticsearch (if using Homebrew): `brew services start elasticsearch`

#### Install on Mac OS X with Homebrew

`brew install elasticsearch`

In `/usr/local/opt/elasticsearch/config/elasticsearch.yml` set `cluster.name: es_dev`

`ln -sfv /usr/local/opt/elasticsearch/*.plist ~/Library/LaunchAgents` (optional, launch
automatically)

`launchctl load ~/Library/LaunchAgents/homebrew.mxcl.elasticsearch.plist` (launch now, do this
outside of tmux)

#### For non-Homebrew installations

Please find your elasticsearch.yml file, set `cluster.name: catalog`, and launch as appropriate.

### Optional

[Marvel](https://www.elastic.co/products/marvel) is a helpful tool for managing and querying
Elasticsearch clusters.
