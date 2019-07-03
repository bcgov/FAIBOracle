<!-- Add a project state badge
See https://github.com/BCDevExchange/Our-Project-Docs/blob/master/discussion/projectstates.md
If you have bcgovr installed and you use RStudio, click the 'Insert BCDevex Badge' Addin. -->

FAIBOracle
==========

The package is used to load data from three major forest inventory
databases in Forest Analysis and Inventory Branch: VGIS, GYS and ISMC.
VGIS is the database for ground sample data in NVAF, VRI phase II,
changing monitoring inventory (CMI), young stand monitoring (YSM) and
Lidar projects (L and B). GYS is the home for natural growth PSP sample
data and TSPs. ISMC stands for Inventory Sample Management
Consolidation, and is the home for both data from VGIS and GYS.
Currently, ISMC is under development.

### Installation

The package is only can be installed and ran in 64-bit R. Because the
package is built on a 64-bit R and 64-bit ROracle client.

To install our package, **ROracle** package must be installed in your R
environment. Installation of is tricky, as it involves the following
steps:

-   Install a 64-bit Oracle client on your machine if it is not
    available. A 64-bit Oracle client can be downloaded
    [here](http://www.oracle.com/technetwork/database/database-technologies/instant-client/downloads/index.html),
    follow the instruction
    [here](https://technology.amis.nl/2017/08/23/r-and-the-oracle-database-using-dplyr-dbplyr-with-roracle-on-windows-10/),
    also showing below:

    -   Download BASE into your target folder, say C:/oracle. In you
        target folder, you will find a folder called instantclient\_?*?.
        Unzip SDK zip file into instantclient*?*? and you will have sdk
        folder in instantclient*?\_?.

    -   Download and install RTools
        [here](https://cran.r-project.org/bin/windows/Rtools/)

-   Download an ROracle from CRAN on
    [here](https://cran.r-project.org/web/packages/ROracle/index.html)
    and save it to a target path, say C:/your/path/to/roracle

-   Open CMD prompt and make some specifications
    -   set PATH=C:\\oracle\\instantclient\_?\_?
    -   set OCI\_LIB64=C:\\oracle\\instantclient\_?\_?
    -   set OCI\_INC=C:\\oracle*?*?\\sdk\\include
-   Install ROracle in CMD prompt using
    -   “C:/path/to/R/bin/x64/R” CMD INSTALL –build
        “C:/Your/Path/To/oracle/ROracle\_1.1-10.tar.gz”

### Usage

#### Example

This is a basic example which shows you how to solve a common problem:

``` r
## basic example code
```

### Project Status

### Getting Help or Reporting an Issue

To report bugs/issues/feature requests, please file an
[issue](https://github.com/bcgov/FAIBOracle/issues/).

### How to Contribute

If you would like to contribute to the package, please see our
[CONTRIBUTING](CONTRIBUTING.md) guidelines.

Please note that this project is released with a [Contributor Code of
Conduct](CODE_OF_CONDUCT.md). By participating in this project you agree
to abide by its terms.

### License

    Copyright 2019 Province of British Columbia

    Licensed under the Apache License, Version 2.0 (the &quot;License&quot;);
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and limitations under the License.

------------------------------------------------------------------------

*This project was created using the
[bcgovr](https://github.com/bcgov/bcgovr) package.*
