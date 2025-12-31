# PatchFox Data Service API Documentation

## Overview

This microservice provides two complementary APIs for accessing data:

1. **Spring Data REST endpoints** - Standard REST API with HATEOAS support
2. **Custom QueryDSL endpoints** - Advanced querying with dynamic filtering

**Base URL:** `{HOST}/api/v1/db`

---

## Entity Overview

The system manages software package data with security vulnerability information:

- **Package** - Software packages with version and vulnerability data
- **Finding** - Security vulnerabilities with severity classifications. Functionally it is a container for a combination of "FindingData" and a collection of "FindingReporter" objects. 
- **FindingData** - Detailed vulnerability information (CVE data, descriptions, etc.)
- **FindingReporter** - Sources that report vulnerabilities
- **Dataset** - Collection of "Datasources". A "Dataset" can represent any grouping of "Datasources"; an entire organization, departments within the organization, groups of "Datasources" that together constitute a product. Anything. 
- **DatasetMetrics** - Analytical metrics and recommendations for datasets
- **Datasource** - Sources of package manifest data (git tracked build files, etc.)
- **DatasourceEvent** - Individual events from a "Datasource" Contains an SBOM, a git-annotated build file, a JSON metadata file. 
- **Edit** - Package changes always tied to a "DatasetMetrics" record. If tied to a "DatasetMetrics" record, the Edit records constitute either actual, projected, or recommended edits to the packages in the "Dataset", as the boolean flag in the "DatasetMetrics" record indicates. 

---

## Spring Data REST Endpoints

### What You Can Do

**Endpoint Pattern:** `{HOST}/api/v1/db/{tableName}`
- **Table names must be camelCase** (e.g., `datasetMetrics`, `findingReporter`)

#### Standard REST Operations
- **Browse collections** with built-in pagination
- **Retrieve individual entities** by ID
- **Navigate relationships** via HATEOAS links
- **Basic filtering** using QueryDSL web support

#### HATEOAS Navigation
Spring Data REST automatically exposes entity relationships as navigable links:

```json
{
  "_links": {
    "self": { "href": "/api/v1/db/package/123" },
    "findings": { "href": "/api/v1/db/package/123/findings" },
    "datasourceEvents": { "href": "/api/v1/db/package/123/datasourceEvents" }
  }
}
```

#### Built-in Pagination and Sorting
```http
GET /api/v1/db/package?page=0&size=20&sort=name,asc
GET /api/v1/db/datasetMetrics?sort=commitDateTime,desc&page=1&size=10
```

#### Basic QueryDSL Integration
Spring Data REST has basic QueryDSL integration for simple filtering:

**Simple Exact Matches Only:**
```http
GET /api/v1/db/package?name=lodash&type=npm
GET /api/v1/db/datasetMetrics?isCurrent=true
GET /api/v1/db/findingData?severity=CRITICAL
```

**String Contains Search (without wildcards):**
```http
GET /api/v1/db/package?name=spring          # Finds packages containing "spring"
GET /api/v1/db/findingData?description=SQL  # Finds descriptions containing "SQL"
```

**Multiple Simple Filters (AND logic):**
```http
GET /api/v1/db/package?type=npm&name=lodash
GET /api/v1/db/datasource?type=maven&domain=github.com
```

### Spring Data REST Response Format
```json
{
  "_embedded": {
    "package": [...]
  },
  "_links": {
    "self": { "href": "..." },
    "next": { "href": "..." }
  },
  "page": {
    "size": 20,
    "totalElements": 1000,
    "totalPages": 50,
    "number": 0
  }
}
```

### Limitations of Spring Data REST

1. **Simple Filtering Only**
   - Only supports exact equality matches and basic string contains search
   - **No numeric comparisons** (>, <, >=, <=, between)
   - **No date range filtering**
   - **No wildcard pattern matching** (*, ?)
   - **No null/not-null checks**

2. **Circular Reference Issues**
   - May encounter JSON serialization problems with deeply nested relationships
   - HATEOAS helps but doesn't eliminate all circular reference scenarios

3. **Limited Query Complexity**
   - Cannot perform aggregations or statistical operations
   - No OR logic support
   - Cannot filter on related entity fields

4. **Fixed Response Structure**
   - Cannot customize which fields are included/excluded beyond HATEOAS
   - Always returns full entity data plus HATEOAS metadata

---

## Custom QueryDSL Endpoint

### What You Can Do

**Endpoint Pattern:** `{HOST}/api/v1/db/{tableName}/query`
- **Table names are case-insensitive** (e.g., `PACKAGE`, `package`, `Package` all work)

#### Available Tables
- `package` / `PACKAGE` / `Package`
- `finding` / `FINDING` / `Finding`
- `findingData` / `FINDINGDATA` / `FindingData`
- `findingReporter` / `FINDINGREPORTER` / `FindingReporter`
- `dataset` / `DATASET` / `Dataset`
- `datasetMetrics` / `DATASETMETRICS` / `DatasetMetrics`
- `datasource` / `DATASOURCE` / `Datasource`
- `datasourceEvent` / `DATASOURCEEVENT` / `DatasourceEvent`
- `edit` / `EDIT` / `Edit`

#### Advanced Filtering Capabilities
The QueryDSL endpoint uses the `QueryDslHelpers` utility to build dynamic queries based on request parameters that map directly to entity fields.

**Exact Matches:**
```http
GET /api/v1/db/package/query?type=npm&name=lodash
GET /api/v1/db/datasetMetrics/query?isCurrent=true
```

**Numeric Comparisons:**
```http
GET /api/v1/db/datasetMetrics/query?totalFindings=gt.100        # Greater than 100
GET /api/v1/db/package/query?numberVersionsBehindHead=lt.5      # Less than 5
GET /api/v1/db/datasetMetrics/query?criticalFindings=gte.10     # Greater than or equal
GET /api/v1/db/datasetMetrics/query?lowFindings=lte.50          # Less than or equal
GET /api/v1/db/datasetMetrics/query?totalFindings=gte.10&gt.100 # Between 10 and 100
```

**Date Range Filtering:**
```http
GET /api/v1/db/datasetMetrics/query?commitDateTime=gt.2024-01-01T00:00:00Z
GET /api/v1/db/datasourceEvent/query?eventDateTime=lt.2024-12-31T23:59:59Z
GET /api/v1/db/datasetMetrics/query?commitDateTime=gte.2024-01-01T00:00:00Z&gt.2024-12-31T23:59:59Z
```

**Pattern Matching with Wildcards:**
```http
GET /api/v1/db/package/query?name=*spring*        # Contains "spring"
GET /api/v1/db/package/query?name=spring*         # Starts with "spring"  
GET /api/v1/db/findingData/query?description=*SQL* # Contains "SQL"
```

**Null Checks:**
```http
GET /api/v1/db/package/query?namespace=null        # Where namespace is null
GET /api/v1/db/package/query?version=notnull       # Where version is not null
```

**Multiple Values (OR logic for same field):**
```http
GET /api/v1/db/package/query?type=npm,maven,pypi   # type is npm OR maven OR pypi
```

**Boolean Fields:**
```http
GET /api/v1/db/datasetMetrics/query?isCurrent=true&isForecastRecommendationsTaken=false
GET /api/v1/db/edit/query?isPfRecommendedEdit=true&isUserEdit=false
```

#### Complex Query Examples

**Find vulnerable npm packages:**
```http
GET /api/v1/db/package/query?type=npm&totalFindings=gt.0
```

**Find current dataset metrics with high critical findings:**
```http
GET /api/v1/db/datasetMetrics/query?isCurrent=true&criticalFindings=gt.10
```

**Find packages with versions:**
```http
GET /api/v1/db/package/query?version=1.0.0,1.1.0,2.0.0
```


## special queryDSL endpoints 

There are many times when the question being asked is tied in with a given Dataset at a given time. For questions involving Packages, Findings, or Edits associated with a given Dataset at a given time, there are the following four endpoints to help. 


**find packages associated wtih a given dataset at a given time**
```http
/api/v1/db/datasetMetrics/package/query
```

**find package types (dedup of find packages) associated wtih a given dataset at a given time**
```http
/api/v1/db/datasetMetrics/packageType/query
```

**find finding types associated wtih a given dataset at a given time**
```http
/api/v1/db/datasetMetrics/package/findingType/query
```

**find edits associated wtih a given dataset at a given time**
```http
/api/v1/db/datasetMetrics/edit/query
```

Every one of these takes a small set of query string arguments intended for a DatasetMetrics query. Any remaining arguments will be passed along to the subsequent query to the Package, Finding, or Edit datastores respectively. These arguments are (and they must be camelCased):

* __datasetName__ (required)
  * the name of the Dataset or Datasets you want to include in the query
  * single name ex `datasetName=foo` 
  * multiple names ex `datasetName=foo,bar`
* __commitDateTime__ (optional)
  * the specific commitDateTime(s) you want records for. If not supplied you will get the latest record 
  * single date exact ex `commitDateTime=2025-04-09T01:08:40.648Z`
  * on or after date ex `commitDateTime=gte.2025-04-09T01:08:40.648Z`
  * range ex `commitDateTime=gte.2025-04-09T01:08:40.648Z&commitDateTime=lt.gte.2025-05-09T01:08:40.648Z`
* __{isCurrent, isForecastSameCourse, isForecastRecommendationsTaken}__ (one of required)
  * indicates what kind of Dataset information you want: the actual data, the forecast based on the actual data, or the recommendations made based on the actual and forecasted data. 
  * just the actual data ex `isCurrent=true`
  * the actual and forecast data `isCurrent=true&isForecastSameCourse=true`

For example, if you make the following call 

```
/api/v1/db/datasetMetrics/package/query?datasetName=foo&isCurrent=true&purl=bar
```

PatchFox will retrieve the most recent metrics record for dataset "foo" marked "is_current". It will then look at all the Packages associated with that record for anything with a field "purl" that contains the text "bar" and return those to the caller. 


