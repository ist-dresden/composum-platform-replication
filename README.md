# [Composum](https://www.composum.com/home.html)

An [Apache Sling](https://sling.apache.org/) based Application Platform

## [Composum Platform Replication](https://www.composum.com/home/platform/extensions/replication.html)

Composum Platform subproject for replicating content remotely to publish hosts

The Composum Platform Replication extension provides the functionality to automatically replicate the published content
of a website from the author server to one or more publish servers. It allows for the separation of concerns by
deploying the application code on the publish servers and replicating the site's content from the author server. This is
particularly useful for sites with high load, as it ensures efficient serving of the site while minimizing downtime
or performance issues.

The module handles the transfer of content in a transactional and efficient manner. It breaks down the transfer into
manageable parts and only transfers the actual changes rather than the entire site. It takes into account various facets
of the content, such as versionables, parent node attributes, and sibling ordering. The transfer process is implemented
using a replication servlet and involves operations such as creating a temporary directory on the publish server,
comparing content between the author and publish servers, and transmitting new, updated, or deleted versionables.

One notable difference between Composum's replication approach and that of Adobe AEM is the concept of releases.
In Composum, pages are grouped into releases, and the publication of a release ensures consistency among the pages.
This allows for easy deployment or rollback of content changes that affect multiple pages and enables automatic
synchronization between servers. Composum also provides the flexibility to emulate AEM's individual page publication
behavior if desired.

More information is available in the
[Composum Platform Replication Documentation](https://composum.com/pages/documentation/replication_to_a_publish_server.html) .

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

### See also

* [Composum Homepage](https://www.composum.com/home/pages.html)
* [Composum Nodes](https://github.com/ist-dresden/composum)
* [Composum Pages](https://www.composum.com/home/pages.html)
* [Composum Assets](https://github.com/ist-dresden/composum-assets)
* [Composum Platform](https://www.composum.com/home/platform.html)
