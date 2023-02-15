<-- LIST| prefix indicates multiple entries may be generated for the element
    OS| prefix indicates include the element if SUPPLIER is TOS or AOS
    FUZZED| prefix indicates include the element if REMARKS contains "fuzzed" (case-insensitive)
    COMMASEP| prefix indicates generate a comma-separated list of values for the element
    EC| prefix indicates include the element if DP_IDQ is DP4.00200.001
    AOP| prefix indicates include the element if SUPPLIER is AOP
    IS| prefix indicates include the element if SUPPLIER is TIS or AIS (excluding EC)
    IS/OS| prefix indicates include the element if SUPPLIER is TIS or AIS (excluding EC), TOS, or AOS
    NON-AOP| prefix indicates include the element if SUPPLIER is not AOP
    NON-NULL| prefix indicates include the element if the value referenced in brackets is non-null
-->
This data package was produced by and downloaded from the National Ecological Observatory Network (NEON). NEON is funded by the National Science Foundation (Awards 0653461, 0752017, 1029808, 1138160, 1246537, 1638695, 1638696, 1724433) and managed cooperatively by Battelle. These data are provided under the terms of the NEON data policy at https://www.neonscience.org/data-policy.

DATA PRODUCT INFORMATION
------------------------

ID: [DP_IDQ]

Name: [DP_NAME]

Description: [DP_DESC]

NEON Science Team Supplier: [SUPPLIER]

Abstract: [DP_ABSTRACT]

Brief Design Description: [DESIGN_DESC]

Brief Study Area Description: [STUDY_DESC]

[NON-NULL|Sensor(s): [SENSOR]]

Keywords: [KEYWORDS]


QUERY INFORMATION
-----------------

[NON-AOP|Date-Time for Data Publication: [QUERY DATE AS YYYY-MM-DD HH:MM (UTC)]]
[NON-AOP|Start Date-Time for Queried Data: [EML TEMPORALCOVERAGE BEGINDATE AS YYYY-MM-DD HH:MM (UTC)]]
[NON-AOP|End Date-Time for Queried Data: [EML TEMPORALCOVERAGE ENDDATE AS YYYY-MM-DD HH:MM (UTC)]]

Site: [SITE]
[NON-AOP|Geographic coordinates (lat/long datum): [EML BOUNDINGCOORDINATES]]
Domain: [DOMAIN]


DATA PACKAGE CONTENTS
---------------------

This folder contains the following documentation files:

- This readme file: [NEON.DOM.SITE.DPL.DPNUM.REV.readme.GENTIME.txt]
[IS/OS|- Term descriptions, data types, and units: [NEON.DOM.SITE.DPL.DPNUM.REV.variables.GENTIME.csv]]
[OS|- Data entry validation and parsing rules: [NEON.DOM.SITE.DPL.DPNUM.REV.validation.GENTIME.csv]]
[OS|- Sampling location files: ]
[LIST|DP_SPEC.DP_SPEC_TITLE WHERE DP_SPEC.TYPE_ID IS TAXONOMY TYPE OR STATUSCODES TYPE]
[NON-AOP|- Machine-readable metadata file describing the data package: [NEON.DOM.SITE.DPL.DPNUM.REV.EML.BEGINDATE-ENDDATE.GENTIME.xml]. This file uses the Ecological Metadata Language schema. Learn more about this specification and tools to parse it at https://www.neonscience.org/about/faq.]
[IS|- Sensor position information: [NEON.DOM.SITE.DPL.PRNUM.REV.sensor_positions.GENTIME.csv]]
- Other related documents, such as engineering specifications, field protocols and data processing documentation, are available. Please visit https://data.neonscience.org/data-products/[DPL.PRNUM.REV] for more information.


This folder also contains [NUMBER OF DATA FILES] data files:
[LIST|FILENAME[NON-NULL| - [TABLE DESCRIPTION]]]

Basic download package definition: [BASIC_DESC]

[NON-NULL|Expanded download package definition: [EXPANDED_DESC]]


FILE NAMING CONVENTIONS
-----------------------

NEON data files are named using a series of component abbreviations separated by periods. File naming conventions for NEON data files differ between NEON science teams. A file will have the same name whether it is accessed via NEON's data portal or API. Please visit https://www.neonscience.org/data-formats-conventions for a full description of the naming conventions.

ISSUE LOG
----------

This log provides a list of issues that were identified during data collection or processing, prior to publication of this data package. For a more recent log, please visit this data product's detail page at https://data.neonscience.org/data-products/[DPL.PRNUM.REV].

[LIST|Issue Date: [DP_CHANGE_LOG.ISSUE_DATE]\nIssue: [DP_CHANGE_LOG.ISSUE]\n[LIST|       Date Range: [DP_CHANGE_LOG.DATE_RANGE_START] to [DP_CHANGE_LOG.DATE_RANGE_END]\n       Location(s) Affected: [COMMASEP|DP_CHANGE_LOG.LOCATION_AFFECTED]]\nResolution Date: [DP_CHANGE_LOG.RESOLVED_DATE]\nResolution: [DP_CHANGE_LOG.RESOLUTION]\n]

ADDITIONAL INFORMATION
----------------------

[FUZZED|Protection of species of concern: At most sites, taxonomic IDs of species of concern have been 'fuzzed', i.e., reported at a higher taxonomic rank than the raw data, to avoid publishing locations of sensitive species. For a few sites with stricter regulations (e.g., Great Smoky Mountains National Park (GRSM)), records for species of concern are not published.]

[DP_CATALOG.REMARKS]

NEON DATA POLICY AND CITATION GUIDELINES
----------------------------------------

A citation statement is available in this data product's detail page at https://data.neonscience.org/data-products/[DPL.PRNUM.REV]. Please visit https://www.neonscience.org/data-policy for more information about NEON's data policy and citation guidelines.

DATA QUALITY AND VERSIONING
---------------------------

NEON data are initially published with a status of Provisional, in which updates to data and/or processing algorithms will occur on an as-needed basis, and query reproducibility cannot be guaranteed. Once data are published as part of a Data Release, they are no longer provisional, and are associated with a stable DOI.

To learn more about provisional versus released data, please visit https://www.neonscience.org/data-revisions-releases.