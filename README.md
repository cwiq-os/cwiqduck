# cwiqduck

## What is cwiqduck?
cwiqduck extension overrides DuckDB's Filesystem Interface. When loaded, this extension changes the behavior of read operations within [CWIQ FS](https://www.codewilling.com/product/cwiq-fs/).

## How does cwiqduck work?
Once loaded, cwiqduck checks if the file in question is within CWIQ FS. If so, the extension redirects DuckDB to the blob storage URL for that file. Then, the httpfs module does the read for that file instead. Diagram below visualizes the extension's capabilities.

<img width="1238" height="715" alt="duckdb_illustration" src="https://github.com/user-attachments/assets/4ca55517-d811-45be-849e-84251856e559" />

## Getting Started
cwiqduck currently does not add any user-defined function. Instead, it tries to convert DuckDB's reads within CWIQ FS to a URL provided by the blob storage provider (such as Amazon S3).

<pre> 
INSTALL httpfs;
INSTALL cwiqduck FROM community;
LOAD cwiqduck;
cwiqduck extension enabled
</pre>

## Dependencies
- httpfs

## Limitation
The cwiqduck extension does not support any non-Linux platform. Moreover, this extension will not handle any filesystem operations outside CWIQ FS mount.
