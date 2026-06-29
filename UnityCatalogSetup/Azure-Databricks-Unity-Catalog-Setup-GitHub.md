# Azure Databricks with Unity Catalog — Complete Setup Guide

> **Goal:** By the end of this guide you will have a fully configured Azure Databricks workspace with Unity Catalog enabled, external storage credentials wired up, and Delta tables created inside a managed catalog.

---

## Table of Contents

0. [Unity Catalog Setup Methods — At a Glance](#0-unity-catalog-setup-methods--at-a-glance)
1. [Architecture Overview](#1-architecture-overview)
2. [Prerequisites](#2-prerequisites)
3. [Step 1 — Create an ADLS Gen2 Storage Account](#3-step-1--create-an-adls-gen2-storage-account)
4. [Step 2 — Create the Unity Catalog Root Container](#4-step-2--create-the-unity-catalog-root-container)
5. [Step 3 — Create an Azure Databricks Workspace (Premium)](#5-step-3--create-an-azure-databricks-workspace-premium)
6. [Step 4 — Create an Access Connector for Azure Databricks](#6-step-4--create-an-access-connector-for-azure-databricks)
7. [Step 5 — Assign Storage Permissions to the Access Connector](#7-step-5--assign-storage-permissions-to-the-access-connector)
8. [Step 6 — Create & Configure the Unity Catalog Metastore](#8-step-6--create--configure-the-unity-catalog-metastore)
9. [Step 7 — Assign the Metastore to the Workspace](#9-step-7--assign-the-metastore-to-the-workspace)
10. [Step 8 — Create External Location & Storage Credential](#10-step-8--create-external-location--storage-credential)
11. [Step 9 — Create Catalog, Schema (Database), and Delta Tables](#11-step-9--create-catalog-schema-database-and-delta-tables)
    - Step 9a — Create Catalog (with / without Managed Location, ALTER)
    - Step 9b — Create Schemas (with / without Managed Location, ALTER)
    - Step 9c — Create Managed Delta Tables (storage resolution waterfall)
    - [Managed vs External Tables — Complete Guide](#11b-managed-vs-external-tables--complete-guide)
12. [Step 10 — Cluster Configuration for Unity Catalog](#12-step-10--cluster-configuration-for-unity-catalog)
13. [Step 11 — Verify Delta Tables in Unity Catalog](#13-step-11--verify-delta-tables-in-unity-catalog)
14. [Governance — Grants & Privileges Reference](#14-governance--grants--privileges-reference)
15. [Common Errors & Fixes](#15-common-errors--fixes)

---

## 0. Unity Catalog Setup Methods — At a Glance

Azure Databricks supports two distinct ways to get Unity Catalog working. Understanding the difference upfront determines how much control you have over your data, storage costs, and cross-tool accessibility.

---

### Method 1 — Default (Auto-Enabled) Unity Catalog

When you create a **new Premium Databricks workspace** today, Unity Catalog is automatically enabled:

- Databricks creates a **regional metastore** in the Databricks Account for your region
- The metastore root storage is **Databricks-owned cloud infrastructure** — it does NOT appear in your Azure subscription
- The `main` catalog is auto-created and linked to the workspace
- You get governance (catalog/schema/table hierarchy, RBAC, lineage) immediately — **but Databricks controls the underlying storage for managed tables**

**To create external tables later**, you still need to manually add an Access Connector + External Locations (Steps 4–8 of this guide).

> **Important — Two conditions must be met for Method 1 to auto-create the ADLS storage account and enable Unity Catalog:**
>
> 1. **Choose "Hybrid" compute mode** (not Serverless) when creating the workspace. With Serverless selected, Databricks does not provision the managed ADLS storage automatically.
> 2. **Create a new, dedicated Resource Group for the Databricks workspace** (do not deploy into an existing resource group). Databricks injects a managed resource group during workspace creation, and an existing resource group can cause the automatic storage provisioning to fail.
>
> If either condition is missed, the auto-setup will not complete and Unity Catalog will not be enabled by default — you will need to follow Method 2 instead.

---

### Method 2 — Manual Setup (This Guide)

You explicitly provision and wire up every component:

- Create your **own ADLS Gen2** in your Azure subscription as the metastore root
- Create an **Access Connector** (Managed Identity) and assign 4 IAM roles
- Manually create the metastore pointing to **your ADLS container**
- Create **External Locations** (bronze/silver/gold) and Storage Credentials upfront
- Managed tables land in **your ADLS**, visible in Azure Portal and accessible by any Azure service

---

### Functional Differences

| Dimension | Method 1 — Default (Auto) | Method 2 — Manual (This Guide) |
|---|---|---|
| **Managed table storage** | Databricks-owned infra — NOT visible in your Azure Portal | Your ADLS Gen2 — visible in Azure Storage Browser |
| **Cost visibility** | Storage cost hidden inside Databricks billing | All storage in your Azure subscription — visible in Azure Cost Management |
| **Access outside Databricks** | Managed tables not directly accessible via ADLS APIs, ADF, Synapse | Managed tables in YOUR ADLS — readable by any Azure service |
| **Storage account ownership** | Databricks owns the metastore root | You own it — control retention, networking, encryption policies |
| **External tables setup** | Must still add Access Connector + External Locations manually later | Pre-configured upfront as part of this setup |
| **DROP TABLE on managed tables** | Deletes files from Databricks storage (you had no direct access anyway) | Deletes files from YOUR ADLS permanently |
| **Data sovereignty** | If Databricks account is suspended, managed table data is at risk | Data always in your Azure subscription regardless of Databricks account status |
| **Network / private endpoint control** | No control over metastore root storage network settings | You configure firewall, private endpoints on your own storage account |
| **Conflict with existing metastore** | This IS the default metastore — one per region per account | If a default metastore already exists in the region, you must reuse and configure it — cannot create a second one |
| **Setup effort** | Zero config | 10+ explicit steps |

---

### The Critical Behavioral Difference — Where Do Managed Tables Land?

```
Method 1 — Default (no MANAGED LOCATION set):

  CREATE TABLE dev_catalog.bronze.customers ...
  → Data goes to Databricks Default Storage (not your ADLS)
  → DESCRIBE DETAIL shows an internal Databricks path
  → Not visible in Azure Portal → no direct ADLS access

Method 2 — Manual (MANAGED LOCATION = your ADLS subfolder):

  CREATE CATALOG dev_catalog
  MANAGED LOCATION 'abfss://unity-catalog-root@youraccount.dfs.core.windows.net/dev_catalog/'

  CREATE TABLE dev_catalog.bronze.customers ...
  → Data lands under: abfss://unity-catalog-root@youraccount.dfs.core.windows.net/dev_catalog/__unitystorage/...
  → Visible in Azure Portal Storage Browser
  → Accessible via ADLS APIs, ADF, Synapse Analytics
```

> **Note:** Even in Method 2, if you create a catalog or schema **without** a `MANAGED LOCATION` clause, Databricks falls back to its own Default Storage. The `MANAGED LOCATION` setting on the catalog/schema is what routes managed tables into YOUR ADLS. See [Step 9a](#11-step-9--create-catalog-schema-database-and-delta-tables) for the complete storage resolution rules.

---

### When to Use Which

| Scenario | Recommended Method |
|---|---|
| POC, sandbox, learning, quick demo | Method 1 — zero config |
| Production data lake with medallion architecture | Method 2 — your data, your storage, your governance |
| Data must be accessible by Synapse, ADF, or non-Databricks tools | Method 2 — external tables on your ADLS |
| Regulated industry (GDPR, HIPAA) — data residency required | Method 2 — you control storage network isolation and encryption |
| Enterprise with Azure Cost Management chargeback | Method 2 — storage costs appear in your Azure bill |

**Bottom line:** Method 1 gives you governance immediately but Databricks owns the managed table storage. Method 2 gives you the same governance **plus full data sovereignty** — your storage, your costs, your access policies. This guide implements Method 2.

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Azure Subscription                           │
│                                                                     │
│  ┌──────────────────────┐      ┌──────────────────────────────────┐ │
│  │  ADLS Gen2           │      │  Azure Databricks Workspace      │ │
│  │  Storage Account     │◄────►│  (Premium Tier)                  │ │
│  │                      │      │                                  │ │
│  │  ┌────────────────┐  │      │  ┌────────────────────────────┐  │ │
│  │  │ unity-catalog/ │  │      │  │  Unity Catalog Metastore   │  │ │
│  │  │ (root container│  │      │  │  (Account Level)           │  │ │
│  │  │  for metastore)│  │      │  └────────────────────────────┘  │ │
│  │  └────────────────┘  │      │                                  │ │
│  │  ┌────────────────┐  │      │  ┌────────────────────────────┐  │ │
│  │  │ external-data/ │  │      │  │  Catalog > Schema > Tables │  │ │
│  │  │ (bronze/silver │  │      │  │  (Delta Tables)            │  │ │
│  │  │  /gold layers) │  │      │  └────────────────────────────┘  │ │
│  │  └────────────────┘  │      └──────────────────────────────────┘ │
│  └──────────────────────┘                   ▲                       │
│                                             │ Managed Identity       │
│  ┌──────────────────────────────────────────┴────────────────────┐  │
│  │           Access Connector for Azure Databricks               │  │
│  │           (System-Assigned Managed Identity)                  │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

**Key Concepts:**

| Component | Purpose |
|---|---|
| ADLS Gen2 | Underlying storage for metastore root + external data |
| Access Connector | Azure resource that gives Databricks a managed identity to access storage |
| Unity Catalog Metastore | Account-level metadata store (one per region) |
| External Location | Maps an ADLS Gen2 path to Unity Catalog so it can be accessed securely |
| Storage Credential | Wraps the Access Connector identity used by External Locations |
| Catalog | Top-level namespace (replaces `hive_metastore`) |
| Schema | Database inside a catalog |
| Delta Table | Managed or External table registered in Unity Catalog |

---

## 2. Prerequisites

| Requirement | Details |
|---|---|
| Azure Subscription | Contributor or Owner role |
| Databricks Account | `accounts.azuredatabricks.net` — Account Admin role |
| Azure AD | Global Admin or User Admin (to assign roles) |
| Region | Choose a region and stay consistent for all resources |
| Databricks Premium | Unity Catalog requires **Premium** tier workspace |

> **Note:** Unity Catalog is managed at the **Databricks Account** level, not the workspace level. You must have an account at `accounts.azuredatabricks.net`.

---

## 3. Step 1 — Create an ADLS Gen2 Storage Account

This storage account serves two purposes:
- **Root storage** for the Unity Catalog metastore
- **External storage** for your data lake (bronze/silver/gold)

### Azure Portal Steps

1. Go to **Azure Portal** → Search `Storage accounts` → Click **+ Create**
2. Fill in the **Basics** tab:

| Field | Value |
|---|---|
| Subscription | Your subscription |
| Resource Group | `rg-databricks-uc` (create new) |
| Storage account name | `stunitycatalog<unique>` (e.g., `stunitycatalogdev01`) |
| Region | e.g., `East US 2` |
| Performance | **Standard** |
| Redundancy | **LRS** (or GRS for production) |

3. Click **Advanced** tab:
   - Enable **Hierarchical namespace** = **Enabled** ← (This makes it ADLS Gen2)

4. Leave **Networking**, **Data protection**, **Encryption** as defaults for dev.

5. Click **Review + Create** → **Create**

---

## 4. Step 2 — Create the Unity Catalog Root Container

The metastore needs a dedicated container. **Do not mix metastore root with your data.**

### Portal Steps

1. Open the storage account → **Containers** (left panel) → **+ Container**
2. Create the following containers:

| Container Name | Purpose |
|---|---|
| `unity-catalog-root` | Metastore root (system metadata) |
| `bronze` | Raw ingestion data layer |
| `silver` | Cleansed/transformed layer |
| `gold` | Aggregated/serving layer |

3. Set **Public access level** = **Private** for all containers.

---

## 5. Step 3 — Create an Azure Databricks Workspace (Premium)

Unity Catalog **requires** the Premium pricing tier.

### Portal Steps

1. Go to **Azure Portal** → Search `Azure Databricks` → **+ Create**

2. Fill in the **Basics** tab:

| Field | Value |
|---|---|
| Subscription | Your subscription |
| Resource Group | `rg-databricks-uc` |
| Workspace name | `adb-unity-catalog-dev` |
| Region | Same as storage account (`East US 2`) |
| Pricing Tier | **Premium** ← Required for Unity Catalog |

3. **Networking** tab:
   - For dev: leave **No VNet injection** (default)
   - For production: select **Custom VNet** and provide subnet details

4. Click **Review + Create** → **Create**

   > Deployment takes ~3–5 minutes.

5. Once deployed, open the workspace → Click **Launch Workspace** to ensure it opens correctly.

---

## 6. Step 4 — Create an Access Connector for Azure Databricks

The **Access Connector** is an Azure resource that provides a **managed identity** (service principal) that Databricks uses to access ADLS Gen2 securely — without storing credentials.

### Portal Steps

1. Go to **Azure Portal** → Search `Access Connector for Azure Databricks` → **+ Create**

2. Fill in:

| Field | Value |
|---|---|
| Subscription | Your subscription |
| Resource Group | `rg-databricks-uc` |
| Name | `adb-access-connector-dev` |
| Region | Same region (`East US 2`) |

3. Click **Review + Create** → **Create**

4. Once deployed, open the Access Connector resource:
   - Go to **Identity** (left panel)
   - Confirm **System-assigned managed identity** status = **On**
   - **Copy the Object (principal) ID** — you will need this for role assignment

---

## 7. Step 5 — Assign Storage Permissions to the Access Connector

The Access Connector's managed identity needs **4 roles** assigned correctly. Assigning only `Storage Blob Data Contributor` is not enough — missing roles cause the **"File Events Read Failed"** error during metastore provisioning.

### Required Roles — Full List

| Role | Assign At | Purpose |
|---|---|---|
| `Storage Blob Data Contributor` | Storage Account | Read/write data files in ADLS Gen2 |
| `Storage Account Contributor` | Storage Account | Manage storage account resources |
| `Storage Queue Data Contributor` | Storage Account | Read/write to Azure Storage Queues (file event tracking) |
| `EventGrid EventSubscription Contributor` | **Resource Group** | Create Event Grid subscriptions for file change notifications |

> **Important:** `EventGrid EventSubscription Contributor` must be assigned at the **Resource Group** level, not the storage account level.

---

### Portal Steps — Assign Roles on the Storage Account

Repeat the following for each of the first **3 roles**:

1. Go to your Storage Account (`stunitycatalogdev01`)
2. Click **Access Control (IAM)** → **+ Add** → **Add role assignment**
3. Search for the role name (see table above) → click it → **Next**
4. **Members** tab:
   - Assign access to: **Managed identity**
   - Click **+ Select members**
   - Subscription: your subscription
   - Managed identity: **Access connector for Azure Databricks**
   - Select: `adb-access-connector-dev`
5. Click **Review + Assign** → **Assign**

Assign all three roles this way:
- `Storage Blob Data Contributor`
- `Storage Account Contributor`
- `Storage Queue Data Contributor`

---

### Portal Steps — Assign EventGrid Role on the Resource Group

1. Go to **Azure Portal → Resource Groups → `rg-databricks-uc`**
2. Click **Access Control (IAM)** → **+ Add** → **Add role assignment**
3. Search: `EventGrid EventSubscription Contributor` → click it → **Next**
4. **Members** tab:
   - Assign access to: **Managed identity**
   - Click **+ Select members**
   - Managed identity: **Access connector for Azure Databricks**
   - Select: `adb-access-connector-dev`
5. Click **Review + Assign** → **Assign**

---

### Verify All 4 Roles Are Assigned

1. Go to **Storage Account → Access Control (IAM) → Role assignments** tab
2. Filter by the Access Connector name — confirm these 3 roles appear:
   - `Storage Blob Data Contributor`
   - `Storage Account Contributor`
   - `Storage Queue Data Contributor`
3. Go to **Resource Group → Access Control (IAM) → Role assignments** tab — confirm:
   - `EventGrid EventSubscription Contributor`

> Wait **2–3 minutes** for IAM role propagation before proceeding to metastore creation.

---

## 8. Step 6 — Create & Configure the Unity Catalog Metastore

The **Metastore** is created once per region at the **Databricks Account** level.

### Step 6a — Log in to Databricks Account Console

1. Navigate to: `https://accounts.azuredatabricks.net`
2. Sign in with your Azure AD account that has **Account Admin** role
3. You should see the Databricks Account Console dashboard

> **Cannot log in? — Common issue with personal Microsoft accounts**
>
> If you see the error _"Selected user account does not exist in tenant 'Microsoft Services'"_,
> your account is a personal Microsoft account (e.g. `@outlook.com`, `@gmail.com`) and is not
> recognized as an Azure AD organizational identity. Follow the steps below to resolve this.

#### If you are unable to log in — Create an Entra ID Admin User

**Step 1 — Create a new user in Microsoft Entra ID (Azure AD)**

1. Log in to **Azure Portal** → Search **Microsoft Entra ID** → **Users** → **+ New user** → **Create user**
2. Fill in:

| Field | Value |
|---|---|
| User principal name | `adb-admin@<yourdomain>.onmicrosoft.com` |
| Display name | `ADB Admin` |
| Password | Auto-generate or set manually — **note it down** |

3. Click **Create**

**Step 2 — Assign Global Administrator role to the new user**

1. In **Microsoft Entra ID** → **Roles and administrators**
2. Search for and click **Global Administrator**
3. Click **+ Add assignments** → select `adb-admin@<yourdomain>.onmicrosoft.com` → **Add**

**Step 3 — Log in to Databricks Account Console with the new user**

1. Open a **private / incognito browser window**
2. Navigate to `https://accounts.azuredatabricks.net`
3. Click **Sign in with Microsoft**
4. Enter `adb-admin@<yourdomain>.onmicrosoft.com` and the password from Step 1
5. Azure will prompt you to **change the password on first login** — set a new password and continue
6. Azure will then ask you to **set up Multi-Factor Authentication (MFA)** — follow the steps below before proceeding
7. After MFA is configured you will land on the Databricks Account Console as an **Account Admin**

> **Be ready — MFA Setup is mandatory for Global Administrator accounts on first login.**
> Download the **Microsoft Authenticator** app on your phone before starting Step 3.

#### MFA Setup — Microsoft Authenticator (prompted automatically on first login)

**On your phone — before logging in:**
1. Open the **App Store** (iPhone) or **Google Play Store** (Android)
2. Search for **Microsoft Authenticator** → Install it
3. Open the app → tap **Add account** → choose **Work or school account**

**Back in the browser — when prompted during login:**

1. Azure shows the screen **"More information required"** → click **Next**
2. On the **"Keep your account secure"** page → click **Next**
3. A **QR code** appears on screen
4. On your phone in the Authenticator app → tap **Scan a QR code** → point the camera at the QR code on screen
5. The account `adb-admin@<yourdomain>.onmicrosoft.com` will appear in the app
6. The browser will send a **test notification** to your phone → tap **Approve** on the phone
7. Click **Next** → **Done** in the browser

> After MFA setup, every login with this account will send an approval notification to the Authenticator app.
> Tap **Approve** on your phone to complete the sign-in.

**Step 4 — Assign Account Admin role to your actual user**

Once inside the Account Console with the new admin user, grant your real account access so you do not need the temporary admin user going forward:

1. In the Account Console → **Settings** (bottom-left gear icon) → **User management**
2. Click the **Admins** tab → **Add admin**
3. Search for your actual user email → select it → **Confirm**
4. Your actual user now has **Account Admin** access to the Databricks Account Console

> You can now log out of the temporary `adb-admin` account, open a new browser window,
> and sign in to `https://accounts.azuredatabricks.net` using your actual Azure AD account.

### Step 6b — Create the Metastore

1. In the Account Console, click **Catalog** (left sidebar) or **Data** → **Unity Catalog**
2. Click **Create Metastore**
3. Fill in:

| Field | Value |
|---|---|
| Name | `metastore-eastus2-dev` |
| Region | `eastus2` ← Must match your workspace region |
| ADLS Gen2 path | `abfss://unity-catalog-root@stunitycatalogdev01.dfs.core.windows.net/` |
| Access Connector ID | Full resource ID of `adb-access-connector-dev` |

**How to get the Access Connector Resource ID:**
- Go to the Access Connector resource in Azure Portal → **Overview** → **JSON View** → copy the `id` field
- Format: `/subscriptions/<sub-id>/resourceGroups/rg-databricks-uc/providers/Microsoft.Databricks/accessConnectors/adb-access-connector-dev`

4. Click **Create** — the metastore is now provisioned.

> One metastore per region per account. If you already have one in `eastus2`, skip creation and reuse it.

---

## 9. Step 7 — Assign the Metastore to the Workspace

A metastore must be explicitly linked to each workspace.

### Account Console Steps

1. In the Account Console → **Catalog** → Select your metastore (`metastore-eastus2-dev`)
2. Click **Workspaces** tab → **Assign to workspace**
3. Select `adb-unity-catalog-dev` from the list
4. Click **Assign**

### Verify in the Workspace

1. Open the Databricks workspace: `https://adb-<workspace-id>.azuredatabricks.net`
2. Click **Catalog** (left sidebar)
3. You should see the Unity Catalog explorer with a `main` catalog and `hive_metastore`

---

## 10. Step 8 — Create External Location & Storage Credential

**Storage Credential** wraps the Access Connector identity.
**External Location** maps a specific ADLS Gen2 path to Unity Catalog.

### Step 8a — Create Storage Credential

**Via Databricks UI:**

1. In the workspace → **Catalog** → **External Data** → **Storage Credentials** → **+ Add**
2. Fill in:

| Field | Value |
|---|---|
| Credential name | `sc-adls-dev` |
| Authentication method | **Azure Managed Identity** |
| Access Connector ID | `/subscriptions/<sub-id>/resourceGroups/rg-databricks-uc/providers/Microsoft.Databricks/accessConnectors/adb-access-connector-dev` |

3. Click **Create**

**Via SQL (in a Databricks notebook or SQL editor):**

```sql
CREATE STORAGE CREDENTIAL `sc-adls-dev`
WITH AZURE_MANAGED_IDENTITY (
  CONNECTOR_ID = '/subscriptions/<subscription-id>/resourceGroups/rg-databricks-uc/providers/Microsoft.Databricks/accessConnectors/adb-access-connector-dev'
);
```

### Step 8b — Create External Location for Bronze Layer

**Via Databricks UI:**

1. **Catalog** → **External Data** → **External Locations** → **+ Add**
2. Fill in:

| Field | Value |
|---|---|
| External location name | `ext-loc-bronze` |
| URL | `abfss://bronze@stunitycatalogdev01.dfs.core.windows.net/` |
| Storage credential | `sc-adls-dev` |

3. Click **Create** → Click **Test connection** to verify

**Via SQL:**

```sql
CREATE EXTERNAL LOCATION `ext-loc-bronze`
URL 'abfss://bronze@stunitycatalogdev01.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `sc-adls-dev`);

-- Test the location
VALIDATE STORAGE CREDENTIAL `sc-adls-dev`
ON LOCATION 'abfss://bronze@stunitycatalogdev01.dfs.core.windows.net/';
```

Repeat for silver and gold:

```sql
CREATE EXTERNAL LOCATION `ext-loc-silver`
URL 'abfss://silver@stunitycatalogdev01.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `sc-adls-dev`);

CREATE EXTERNAL LOCATION `ext-loc-gold`
URL 'abfss://gold@stunitycatalogdev01.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `sc-adls-dev`);
```

---

## 11. Step 9 — Create Catalog, Schema (Database), and Delta Tables

### Step 9a — Create a Catalog

> **Critical Rule — Where do managed tables get stored?**
>
> If you create a catalog or schema **without** a `MANAGED LOCATION`, Databricks stores managed tables in its own internal **"Default Storage"** — which is Databricks-owned cloud infrastructure, NOT your ADLS account. You will NOT see those files in Azure Portal.
>
> To store managed tables in **your ADLS root**, you must set `MANAGED LOCATION` to a **subfolder** inside your metastore container — the root path itself cannot be used (causes `LOCATION_OVERLAP` error).

```
❌ No MANAGED LOCATION set anywhere
      → managed tables go to "Default Storage" (Databricks-owned, not visible in your Azure)

✅ MANAGED LOCATION set to a subfolder of your metastore container
      → managed tables go to YOUR ADLS under __unitystorage/ inside that subfolder
```

#### Managed Location Path Rules

| Path | Result |
|---|---|
| `abfss://metastore@storage.dfs.core.windows.net/` | ❌ ERROR — root is reserved for the metastore |
| `abfss://metastore@storage.dfs.core.windows.net/dev_catalog/` | ✅ Subfolder — works correctly |
| `abfss://metastore@storage.dfs.core.windows.net/staging_catalog/` | ✅ Subfolder — works correctly |
| No `MANAGED LOCATION` clause | ⚠️ Tables go to Databricks Default Storage (not your ADLS) |

---

#### Create Catalog — With MANAGED LOCATION Subfolder (Recommended)

Use a unique subfolder name per catalog inside your metastore container:

```sql
-- dev environment catalog
CREATE CATALOG IF NOT EXISTS dev_catalog
MANAGED LOCATION 'abfss://metastore@valaxystadlsunitycatalog.dfs.core.windows.net/dev_catalog/'
COMMENT 'Dev catalog — managed tables stored under dev_catalog/ subfolder in ADLS';

-- staging environment catalog
CREATE CATALOG IF NOT EXISTS staging_catalog
MANAGED LOCATION 'abfss://metastore@valaxystadlsunitycatalog.dfs.core.windows.net/staging_catalog/'
COMMENT 'Staging catalog — managed tables stored under staging_catalog/ subfolder';

-- prod environment catalog
CREATE CATALOG IF NOT EXISTS prod_catalog
MANAGED LOCATION 'abfss://metastore@valaxystadlsunitycatalog.dfs.core.windows.net/prod_catalog/'
COMMENT 'Prod catalog — managed tables stored under prod_catalog/ subfolder';

-- Verify — check "Managed Location" field in the output
DESCRIBE CATALOG EXTENDED dev_catalog;

-- List all catalogs
SHOW CATALOGS;
```

#### Alter an Existing Catalog's Managed Location

```sql
-- Change managed location — applies to NEW tables only, existing data is NOT moved
ALTER CATALOG dev_catalog
SET MANAGED LOCATION 'abfss://metastore@valaxystadlsunitycatalog.dfs.core.windows.net/dev_catalog/';
```

> The `main` catalog is auto-created by Unity Catalog. Always create separate catalogs per environment (dev / staging / prod).

---

### Step 9b — Create Schemas (Databases)

Schemas inside a catalog **inherit** the catalog's `MANAGED LOCATION` automatically. You do not need to set a managed location on each schema unless you want each layer (bronze/silver/gold) stored in a different ADLS container.

#### Option A — Schemas inheriting from catalog (simplest — recommended)

```sql
USE CATALOG dev_catalog;

-- Schemas inherit the catalog's managed location:
-- abfss://metastore@valaxystadlsunitycatalog.dfs.core.windows.net/dev_catalog/
-- All managed tables land under dev_catalog/__unitystorage/schemas/<uuid>/

CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Raw ingested data';

CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Cleansed and conformed data';

CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Aggregated and serving layer';

-- Verify
SHOW SCHEMAS IN dev_catalog;
```

#### Option B — Schemas with separate ADLS containers per layer

Use this only if you want bronze/silver/gold managed tables in completely separate ADLS containers.

```sql
USE CATALOG dev_catalog;

CREATE SCHEMA IF NOT EXISTS bronze
MANAGED LOCATION 'abfss://bronze@valaxystadlsunitycatalog.dfs.core.windows.net/managed/'
COMMENT 'Raw ingested data — managed tables in bronze container';

CREATE SCHEMA IF NOT EXISTS silver
MANAGED LOCATION 'abfss://silver@valaxystadlsunitycatalog.dfs.core.windows.net/managed/'
COMMENT 'Cleansed data — managed tables in silver container';

CREATE SCHEMA IF NOT EXISTS gold
MANAGED LOCATION 'abfss://gold@valaxystadlsunitycatalog.dfs.core.windows.net/managed/'
COMMENT 'Aggregated data — managed tables in gold container';

-- Verify each schema's managed location
DESCRIBE SCHEMA EXTENDED dev_catalog.bronze;
DESCRIBE SCHEMA EXTENDED dev_catalog.silver;
DESCRIBE SCHEMA EXTENDED dev_catalog.gold;
```

#### Alter an Existing Schema's Managed Location

```sql
ALTER SCHEMA dev_catalog.bronze
SET MANAGED LOCATION 'abfss://metastore@valaxystadlsunitycatalog.dfs.core.windows.net/dev_catalog/bronze/';
```

### Step 9c — Create Managed Delta Tables

A managed table has **no LOCATION clause**. Where the data lands depends entirely on whether you set `MANAGED LOCATION` on the catalog or schema in the steps above.

| Catalog/Schema setup | Where managed tables are stored |
|---|---|
| Catalog has `MANAGED LOCATION` subfolder set | Your ADLS under `<subfolder>/__unitystorage/schemas/<uuid>/` |
| Schema has `MANAGED LOCATION` set | Your ADLS under that schema path `/__unitystorage/schemas/<uuid>/` |
| Neither catalog nor schema has `MANAGED LOCATION` | **Databricks Default Storage** — NOT in your ADLS, not visible in Azure Portal |

> **To store managed tables in your ADLS root:** Always create the catalog with `MANAGED LOCATION` pointing to a subfolder inside your metastore container (e.g. `.../dev_catalog/`). Schemas inside that catalog will automatically inherit it.

#### Verify before creating tables — confirm catalog has managed location

```sql
-- Confirm your catalog has a managed location set (not empty)
DESCRIBE CATALOG EXTENDED dev_catalog;
-- Look for: "Managed Location" = abfss://metastore@valaxystadlsunitycatalog.dfs.core.windows.net/dev_catalog/
-- If it shows empty or "Default Storage" → run ALTER CATALOG first (see Step 9a)
```

```sql
-- Step 1: Set catalog and schema context
USE CATALOG dev_catalog;
USE SCHEMA bronze;

-- Step 2: Create managed table — NO LOCATION clause
-- Data will land in:
-- abfss://metastore@valaxystadlsunitycatalog.dfs.core.windows.net/dev_catalog/__unitystorage/schemas/<uuid>/<table-uuid>/
CREATE TABLE IF NOT EXISTS customers (
  customer_id   BIGINT        NOT NULL,
  first_name    STRING        NOT NULL,
  last_name     STRING        NOT NULL,
  email         STRING,
  phone         STRING,
  created_date  DATE,
  country       STRING,
  is_active     BOOLEAN       DEFAULT true
)
USING DELTA
COMMENT 'Customer master data - raw ingestion'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
);

-- Step 3: Insert sample data
INSERT INTO dev_catalog.bronze.customers VALUES
  (1, 'Alice',   'Smith',   'alice@example.com',  '555-0101', '2024-01-15', 'US',  true),
  (2, 'Bob',     'Jones',   'bob@example.com',    '555-0102', '2024-02-20', 'UK',  true),
  (3, 'Charlie', 'Brown',   'charlie@example.com','555-0103', '2024-03-10', 'US',  false);

-- Step 4: Query data
SELECT * FROM dev_catalog.bronze.customers;

-- Step 5: Confirm table is stored in YOUR ADLS (not Default Storage)
DESCRIBE DETAIL dev_catalog.bronze.customers;
-- "location" must start with:
-- abfss://metastore@valaxystadlsunitycatalog.dfs.core.windows.net/dev_catalog/__unitystorage/
-- If it shows "Default Storage" → catalog was created without MANAGED LOCATION (see Step 9a)
```

### Step 9d — Create External Delta Tables

External tables store data in your ADLS Gen2 paths (bronze/silver/gold containers). You manage the data lifecycle.

```sql
-- External Delta Table pointing to ADLS Gen2 bronze container
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.sales_orders
(
  order_id      BIGINT    NOT NULL,
  customer_id   BIGINT    NOT NULL,
  order_date    DATE,
  product_code  STRING,
  quantity      INT,
  unit_price    DECIMAL(10,2),
  total_amount  DECIMAL(10,2),
  status        STRING
)
USING DELTA
LOCATION 'abfss://bronze@stunitycatalogdev01.dfs.core.windows.net/sales_orders/'
COMMENT 'Sales orders - external Delta table on ADLS Gen2'
PARTITIONED BY (order_date);

-- Insert sample data
INSERT INTO dev_catalog.bronze.sales_orders VALUES
  (1001, 1, '2024-06-01', 'PROD-A', 2, 49.99, 99.98,  'COMPLETED'),
  (1002, 2, '2024-06-02', 'PROD-B', 1, 199.00,199.00, 'PENDING'),
  (1003, 1, '2024-06-03', 'PROD-A', 5, 49.99, 249.95, 'COMPLETED');

SELECT * FROM dev_catalog.bronze.sales_orders;
```

### Step 9e — Create Silver Layer Table (with Transformation)

```sql
-- Silver layer: cleansed and joined
CREATE TABLE IF NOT EXISTS dev_catalog.silver.customer_orders
USING DELTA
LOCATION 'abfss://silver@stunitycatalogdev01.dfs.core.windows.net/customer_orders/'
COMMENT 'Joined customer and order data - silver layer'
AS
SELECT
  c.customer_id,
  c.first_name || ' ' || c.last_name  AS customer_name,
  c.country,
  o.order_id,
  o.order_date,
  o.product_code,
  o.quantity,
  o.total_amount,
  o.status
FROM dev_catalog.bronze.customers  c
JOIN dev_catalog.bronze.sales_orders o
  ON c.customer_id = o.customer_id
WHERE c.is_active = true;

SELECT * FROM dev_catalog.silver.customer_orders;
```

### Step 9f — Create Gold Layer Aggregated Table

```sql
-- Gold layer: aggregated metrics
CREATE TABLE IF NOT EXISTS dev_catalog.gold.customer_revenue_summary
USING DELTA
LOCATION 'abfss://gold@stunitycatalogdev01.dfs.core.windows.net/customer_revenue_summary/'
COMMENT 'Customer revenue summary - gold layer'
AS
SELECT
  customer_id,
  customer_name,
  country,
  COUNT(order_id)        AS total_orders,
  SUM(total_amount)      AS total_revenue,
  AVG(total_amount)      AS avg_order_value,
  MAX(order_date)        AS last_order_date
FROM dev_catalog.silver.customer_orders
WHERE status = 'COMPLETED'
GROUP BY customer_id, customer_name, country;

SELECT * FROM dev_catalog.gold.customer_revenue_summary;
```

---

## 11b. Managed vs External Tables — Complete Guide

### Key Differences

| | Managed Table | External Table |
|---|---|---|
| **Storage location** | Databricks controls — stored under metastore root or catalog managed location | You control — stored at a path you specify in ADLS Gen2 |
| **Created with** | `CREATE TABLE ... USING DELTA` (no LOCATION clause) | `CREATE TABLE ... USING DELTA LOCATION 'abfss://...'` |
| **DROP TABLE behaviour** | Deletes **both metadata AND data files** permanently | Deletes **metadata only** — data files remain in ADLS |
| **Data lifecycle** | Fully managed by Databricks | Managed by you |
| **Access outside Databricks** | Not easily — path is internal | Yes — files are in your own ADLS container |
| **Schema enforcement** | Full Unity Catalog enforcement | Full Unity Catalog enforcement |
| **ACID / Time travel** | Yes (Delta) | Yes (Delta) |
| **Best for** | Scratch tables, ML features, intermediate results | Production data lake tables (bronze/silver/gold), shared data |

---

### Step-by-Step: Create a Managed Table

A managed table has **no LOCATION clause** — Databricks picks where to store the data.

**Step 1 — Set the context**

```sql
USE CATALOG dev_catalog;
USE SCHEMA bronze;
```

**Step 2 — Create the managed table using SQL DDL**

```sql
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.customers_managed (
  customer_id   BIGINT        NOT NULL,
  first_name    STRING        NOT NULL,
  last_name     STRING        NOT NULL,
  email         STRING,
  phone         STRING,
  country       STRING,
  created_date  DATE,
  is_active     BOOLEAN       DEFAULT true
)
USING DELTA
COMMENT 'Customer master — managed table, storage owned by Databricks'
TBLPROPERTIES (
  'delta.enableChangeDataFeed'           = 'true',
  'delta.autoOptimize.optimizeWrite'     = 'true'
);
```

**Step 3 — Insert data**

```sql
INSERT INTO dev_catalog.bronze.customers_managed VALUES
  (1, 'Alice',   'Smith',   'alice@example.com',   '555-0101', 'US', '2024-01-15', true),
  (2, 'Bob',     'Jones',   'bob@example.com',     '555-0102', 'UK', '2024-02-20', true),
  (3, 'Charlie', 'Brown',   'charlie@example.com', '555-0103', 'US', '2024-03-10', false);
```

**Step 4 — Verify table and its storage location**

```sql
-- Check data
SELECT * FROM dev_catalog.bronze.customers_managed;

-- Check where Databricks stored the data (under metastore root)
DESCRIBE DETAIL dev_catalog.bronze.customers_managed;
-- The "location" column shows the auto-assigned path under the metastore root
```

**Step 5 — Create managed table using PySpark**

```python
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, BooleanType

schema = StructType([
    StructField("customer_id",  LongType(),    False),
    StructField("first_name",   StringType(),  False),
    StructField("last_name",    StringType(),  False),
    StructField("email",        StringType(),  True),
    StructField("country",      StringType(),  True),
    StructField("created_date", DateType(),    True),
    StructField("is_active",    BooleanType(), True),
])

data = [
    (1, "Alice",   "Smith",   "alice@example.com",   "US", "2024-01-15", True),
    (2, "Bob",     "Jones",   "bob@example.com",     "UK", "2024-02-20", True),
    (3, "Charlie", "Brown",   "charlie@example.com", "US", "2024-03-10", False),
]

df = spark.createDataFrame(data, schema=schema)

# Write as managed table — no path needed
df.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("dev_catalog.bronze.customers_managed")

spark.table("dev_catalog.bronze.customers_managed").show()
```

**Step 6 — Verify DROP deletes data**

```sql
-- WARNING: This permanently deletes both metadata AND data files
DROP TABLE dev_catalog.bronze.customers_managed;

-- The data is gone — cannot be recovered unless you have a backup
```

---

### Step-by-Step: Create an External Table

An external table uses a **LOCATION clause** pointing to your ADLS Gen2 path.
Databricks registers the metadata but you own the files.

**Step 1 — Make sure the External Location is already created (Step 8)**

```sql
-- Verify your external location exists and is accessible
SHOW EXTERNAL LOCATIONS;

-- Test the path is reachable
VALIDATE STORAGE CREDENTIAL `sc-adls-dev`
ON LOCATION 'abfss://bronze@stunitycatalogdev01.dfs.core.windows.net/';
```

**Step 2 — Create the external table using SQL DDL**

```sql
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.sales_orders_external (
  order_id      BIGINT         NOT NULL,
  customer_id   BIGINT         NOT NULL,
  order_date    DATE,
  product_code  STRING,
  quantity      INT,
  unit_price    DECIMAL(10,2),
  total_amount  DECIMAL(10,2),
  status        STRING
)
USING DELTA
LOCATION 'abfss://bronze@stunitycatalogdev01.dfs.core.windows.net/sales_orders/'
COMMENT 'Sales orders — external table, data stored in ADLS bronze container'
PARTITIONED BY (order_date)
TBLPROPERTIES (
  'delta.enableChangeDataFeed'       = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
);
```

**Step 3 — Insert data**

```sql
INSERT INTO dev_catalog.bronze.sales_orders_external VALUES
  (1001, 1, '2024-06-01', 'PROD-A', 2, 49.99,  99.98,  'COMPLETED'),
  (1002, 2, '2024-06-02', 'PROD-B', 1, 199.00, 199.00, 'PENDING'),
  (1003, 1, '2024-06-03', 'PROD-A', 5, 49.99,  249.95, 'COMPLETED'),
  (1004, 3, '2024-06-04', 'PROD-C', 3, 75.00,  225.00, 'COMPLETED');
```

**Step 4 — Verify table and its storage location**

```sql
-- Check data
SELECT * FROM dev_catalog.bronze.sales_orders_external;

-- Confirm location points to your ADLS path
DESCRIBE DETAIL dev_catalog.bronze.sales_orders_external;
-- "location" = abfss://bronze@stunitycatalogdev01.dfs.core.windows.net/sales_orders/
```

**Step 5 — Create external table using PySpark**

```python
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, DecimalType, IntegerType

schema = StructType([
    StructField("order_id",     LongType(),        False),
    StructField("customer_id",  LongType(),        False),
    StructField("order_date",   DateType(),        True),
    StructField("product_code", StringType(),      True),
    StructField("quantity",     IntegerType(),     True),
    StructField("unit_price",   DecimalType(10,2), True),
    StructField("total_amount", DecimalType(10,2), True),
    StructField("status",       StringType(),      True),
])

data = [
    (1001, 1, "2024-06-01", "PROD-A", 2, 49.99,  99.98,  "COMPLETED"),
    (1002, 2, "2024-06-02", "PROD-B", 1, 199.00, 199.00, "PENDING"),
    (1003, 1, "2024-06-03", "PROD-A", 5, 49.99,  249.95, "COMPLETED"),
]

df = spark.createDataFrame(data, schema=schema)

# Write as external table — path is specified
(
    df.write
      .format("delta")
      .mode("overwrite")
      .option("path", "abfss://bronze@stunitycatalogdev01.dfs.core.windows.net/sales_orders/")
      .saveAsTable("dev_catalog.bronze.sales_orders_external")
)

spark.table("dev_catalog.bronze.sales_orders_external").show()
```

**Step 6 — Verify DROP keeps data safe**

```sql
-- Drop the table — removes only the metadata from Unity Catalog
DROP TABLE dev_catalog.bronze.sales_orders_external;

-- Data files still exist in ADLS at:
-- abfss://bronze@stunitycatalogdev01.dfs.core.windows.net/sales_orders/

-- Re-register the same data as a table any time
CREATE TABLE dev_catalog.bronze.sales_orders_external
USING DELTA
LOCATION 'abfss://bronze@stunitycatalogdev01.dfs.core.windows.net/sales_orders/';

SELECT * FROM dev_catalog.bronze.sales_orders_external;
```

---

### CTAS — Create Table As Select (Both Types)

Use `CREATE TABLE ... AS SELECT` when you want to create a table and populate it from a query in one step.

**Managed CTAS**

```sql
-- Creates and populates a managed table from a query result
CREATE TABLE dev_catalog.gold.high_value_customers
USING DELTA
COMMENT 'Customers with total orders > 300'
AS
SELECT
  customer_id,
  first_name || ' ' || last_name AS customer_name,
  country
FROM dev_catalog.bronze.customers_managed
WHERE is_active = true;

SELECT * FROM dev_catalog.gold.high_value_customers;
```

**External CTAS**

```sql
-- Creates and populates an external table at a specific ADLS path
CREATE TABLE dev_catalog.gold.revenue_summary_external
USING DELTA
LOCATION 'abfss://gold@stunitycatalogdev01.dfs.core.windows.net/revenue_summary/'
COMMENT 'Revenue summary — stored in gold ADLS container'
AS
SELECT
  customer_id,
  COUNT(order_id)    AS total_orders,
  SUM(total_amount)  AS total_revenue,
  MAX(order_date)    AS last_order_date
FROM dev_catalog.bronze.sales_orders_external
WHERE status = 'COMPLETED'
GROUP BY customer_id;

SELECT * FROM dev_catalog.gold.revenue_summary_external;
```

---

### DML Operations on Both Table Types

Both managed and external tables support full DML — the behaviour is identical from a query perspective.

```sql
-- UPDATE
UPDATE dev_catalog.bronze.customers_managed
SET is_active = false
WHERE country = 'US' AND customer_id = 3;

-- DELETE
DELETE FROM dev_catalog.bronze.customers_managed
WHERE is_active = false;

-- MERGE (upsert)
MERGE INTO dev_catalog.bronze.customers_managed AS target
USING (
  SELECT 4 AS customer_id, 'Diana' AS first_name, 'Prince' AS last_name,
         'diana@example.com' AS email, '555-0104' AS phone,
         'US' AS country, CAST('2024-07-01' AS DATE) AS created_date, true AS is_active
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

---

### ALTER TABLE — Modify Table Properties

```sql
-- Add a new column
ALTER TABLE dev_catalog.bronze.customers_managed
ADD COLUMN loyalty_tier STRING AFTER country;

-- Rename a column
ALTER TABLE dev_catalog.bronze.customers_managed
RENAME COLUMN phone TO phone_number;

-- Change comment
ALTER TABLE dev_catalog.bronze.customers_managed
SET TBLPROPERTIES ('comment' = 'Updated comment');

-- Enable Change Data Feed on existing table
ALTER TABLE dev_catalog.bronze.sales_orders_external
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

---

### Inspect Tables

```sql
-- List all tables in a schema
SHOW TABLES IN dev_catalog.bronze;

-- Basic column info
DESCRIBE dev_catalog.bronze.customers_managed;

-- Full details: location, format, num files, size
DESCRIBE DETAIL dev_catalog.bronze.customers_managed;
DESCRIBE DETAIL dev_catalog.bronze.sales_orders_external;

-- Full column + table property info
DESCRIBE EXTENDED dev_catalog.bronze.customers_managed;

-- Full history of all operations
DESCRIBE HISTORY dev_catalog.bronze.sales_orders_external;
```

---

### When to Use Which

| Use Case | Table Type |
|---|---|
| Bronze raw ingestion from ADLS | **External** — data already lives in ADLS at a known path |
| Silver cleansed layer | **External** — accessible by other tools (Synapse, ADF) |
| Gold aggregated / serving layer | **External** — shared with downstream consumers |
| Scratch / intermediate computation | **Managed** — Databricks handles cleanup |
| ML feature tables | **Managed** — lifecycle tied to the ML project |
| Tables shared with non-Databricks tools | **External** — files in ADLS can be read by any tool |
| Tables where you must guarantee data deletion on DROP | **Managed** — DROP TABLE removes files too |

---

## 12. Step 10 — Cluster Configuration for Unity Catalog

Unity Catalog has specific cluster requirements.

### Cluster Policy Requirements

| Setting | Required Value |
|---|---|
| Databricks Runtime | **11.3 LTS** or above |
| Access Mode | **Single User** or **Shared** (NOT "No isolation shared") |
| Cluster Mode | Standard (not High Concurrency legacy) |

### Create a UC-Compatible Cluster (UI)

1. In the workspace → **Compute** → **+ Create compute**
2. Configure:

| Field | Value |
|---|---|
| Cluster name | `uc-dev-cluster` |
| Policy | **Personal Compute** or **Unrestricted** |
| Access mode | **Single user** (select your user email) |
| Databricks Runtime | `14.3 LTS` or latest LTS |
| Worker type | `Standard_DS3_v2` (dev) |
| Min workers | `1` |
| Max workers | `3` |
| Enable autoscaling | Yes |

3. Click **Create compute**

> **Important:** "No isolation shared" clusters do NOT support Unity Catalog. Always use Single User or Shared access mode.

### Attach Notebook to Cluster

1. Open a notebook → Top-left cluster dropdown → Select `uc-dev-cluster`
2. Run `SHOW CATALOGS;` to confirm Unity Catalog is accessible

---

## 13. Step 11 — Verify Delta Tables in Unity Catalog

### Verification Queries

```sql
-- 1. List all catalogs
SHOW CATALOGS;

-- 2. List schemas in your catalog
SHOW SCHEMAS IN dev_catalog;

-- 3. List all tables
SHOW TABLES IN dev_catalog.bronze;
SHOW TABLES IN dev_catalog.silver;
SHOW TABLES IN dev_catalog.gold;

-- 4. Inspect table details (location, format, partitions)
DESCRIBE DETAIL dev_catalog.bronze.customers;
DESCRIBE DETAIL dev_catalog.bronze.sales_orders;

-- 5. Check table history (Delta time travel)
DESCRIBE HISTORY dev_catalog.bronze.sales_orders;

-- 6. Read data
SELECT * FROM dev_catalog.gold.customer_revenue_summary;

-- 7. Verify external location paths
SELECT * FROM dev_catalog.bronze.sales_orders VERSION AS OF 1;

-- 8. Check Unity Catalog metadata
SELECT * FROM system.information_schema.tables
WHERE table_catalog = 'dev_catalog';
```

### Python (PySpark) Verification

```python
# In a Databricks notebook
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# List catalogs
spark.sql("SHOW CATALOGS").show()

# Read a Unity Catalog Delta table
df = spark.table("dev_catalog.gold.customer_revenue_summary")
df.show()
df.printSchema()

# Check table location
spark.sql("DESCRIBE DETAIL dev_catalog.bronze.sales_orders").select(
    "name", "location", "format", "numFiles", "sizeInBytes"
).show(truncate=False)
```

---

## 14. Governance — Grants & Privileges Reference

Unity Catalog enforces fine-grained access control at every level.

### Privilege Hierarchy

```
Account Admin
  └── Metastore Admin
        ├── Catalog Owner / USAGE
        │     ├── Schema Owner / USAGE
        │     │     ├── Table SELECT
        │     │     ├── Table MODIFY
        │     │     └── Table ALL PRIVILEGES
        │     └── External Location CREATE EXTERNAL TABLE
        └── Storage Credential CREATE EXTERNAL LOCATION
```

### Common Grant Statements

```sql
-- Grant a user access to a catalog
GRANT USAGE ON CATALOG dev_catalog TO `user@company.com`;

-- Grant a group access to a schema
GRANT USAGE ON SCHEMA dev_catalog.bronze TO `data-engineers`;

-- Grant SELECT on a table to a group
GRANT SELECT ON TABLE dev_catalog.silver.customer_orders TO `data-analysts`;

-- Grant MODIFY (insert/update/delete) to a group
GRANT MODIFY ON TABLE dev_catalog.bronze.customers TO `data-engineers`;

-- Grant all privileges on a schema
GRANT ALL PRIVILEGES ON SCHEMA dev_catalog.gold TO `data-engineers`;

-- Grant CREATE TABLE on a schema
GRANT CREATE TABLE ON SCHEMA dev_catalog.bronze TO `data-engineers`;

-- Grant access to external location
GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `ext-loc-bronze` TO `data-engineers`;

-- View current grants
SHOW GRANTS ON TABLE dev_catalog.bronze.customers;
SHOW GRANTS ON SCHEMA dev_catalog.bronze;
SHOW GRANTS ON CATALOG dev_catalog;
```

---

## 15. Common Errors & Fixes

### A. Account Console Login Errors

| Error | Cause | Fix |
|---|---|---|
| `Selected user account does not exist in tenant 'Microsoft Services'` | Logged in with a personal Microsoft account (@outlook, @gmail etc.) | Create an organizational user in Microsoft Entra ID → assign Global Administrator → log in with that user → assign Account Admin to your real account (see Step 6a) |
| `You do not have permission to access this resource` | User is not an Account Admin in Databricks | Ask the current Account Admin to add you via Account Console → Settings → User management → Admins |

---

### B. Metastore Provisioning Errors

| Error | Cause | Fix |
|---|---|---|
| `File Events Read Failed. Failed to provision file events resources during queue.create` | Access Connector missing one or more of the 4 required IAM roles | Assign all 4 roles: `Storage Blob Data Contributor`, `Storage Account Contributor`, `Storage Queue Data Contributor` on the **Storage Account**; `EventGrid EventSubscription Contributor` on the **Resource Group** (see Step 5) |
| `AuthorizationFailure: This request is not authorized to perform this operation` | IAM role assigned but not yet propagated, or wrong scope | Wait 2–3 minutes and retry; verify roles are on the correct scope (storage account vs resource group) |
| `Metastore not assigned to workspace` | Metastore and workspace not linked | Account Console → Catalog → select metastore → Workspaces tab → Assign |

---

### C. External Location & Storage Credential Errors

| Error | Cause | Fix |
|---|---|---|
| `You do not have the CREATE EXTERNAL LOCATION privilege for this metastore` | User is not Metastore Admin or lacks privilege | Add user as Metastore Admin in Account Console → Catalog → Admins tab, OR run: `GRANT CREATE EXTERNAL LOCATION ON METASTORE TO 'user@domain.com'` |
| `User does not have CREATE MANAGED STORAGE on External Location 'metastore_root_location'` | User cannot create managed tables/catalogs in the metastore root | Run as Metastore Admin: `GRANT CREATE MANAGED STORAGE ON EXTERNAL LOCATION 'metastore_root_location' TO 'user@domain.com'` |
| `External location overlaps with existing external location` | Two external locations with overlapping ADLS paths | Use non-overlapping path prefixes for each external location |
| `Path not accessible: abfss://...` | Access Connector missing `Storage Blob Data Contributor` role | Re-check all 4 IAM role assignments on storage account and resource group (see Step 5) |

---

### D. Managed Catalog / Schema / Table Storage Errors

| Error | Cause | Fix |
|---|---|---|
| `INVALID_PARAMETER_VALUE.LOCATION_OVERLAP: Input path overlaps with managed storage` | Catalog `MANAGED LOCATION` is set to the **root** of the metastore container — which is already reserved | Use a **subfolder**: `abfss://metastore@storage.dfs.core.windows.net/dev_catalog/` instead of the root path |
| Managed table shows **"Default Storage"** instead of your ADLS path | Catalog/schema was created without a `MANAGED LOCATION` — Databricks falls back to its own internal storage | Run `ALTER CATALOG dev_catalog SET MANAGED LOCATION 'abfss://metastore@storage.dfs.core.windows.net/dev_catalog/'` then re-create the table |
| Managed table files not visible in Azure Portal Storage Browser | Table is stored in Databricks Default Storage (not your ADLS) | Set `MANAGED LOCATION` at catalog or schema level pointing to your ADLS subfolder (see Step 9a) |
| `ALTER CATALOG` changes do not affect existing tables | `SET MANAGED LOCATION` only applies to newly created tables | Back up data → DROP old table → re-create table after setting managed location |

```sql
-- Quick diagnostic: check if catalog has managed location set
DESCRIBE CATALOG EXTENDED dev_catalog;
-- "Managed Location" row must show your ADLS subfolder path
-- If empty → tables go to Default Storage (not your ADLS)

-- Fix: set managed location to a subfolder (not the root container)
ALTER CATALOG dev_catalog
SET MANAGED LOCATION 'abfss://metastore@valaxystadlsunitycatalog.dfs.core.windows.net/dev_catalog/';

-- After fix: new managed tables will go to YOUR ADLS
-- Verify after creating a table:
DESCRIBE DETAIL dev_catalog.bronze.customers;
-- location: abfss://metastore@valaxystadlsunitycatalog.dfs.core.windows.net/dev_catalog/__unitystorage/...
```

---

### E. Unity Catalog & Cluster Errors

| Error | Cause | Fix |
|---|---|---|
| `PERMISSION_DENIED: User does not have CREATE CATALOG privilege` | User is not Metastore Admin | Grant Metastore Admin in Account Console → Catalog → Admins tab |
| `Unity Catalog is not enabled for this workspace` | Workspace not assigned to a metastore | Account Console → Catalog → Workspaces → Assign |
| `This operation is not supported for No Isolation Shared clusters` | Wrong cluster access mode | Recreate cluster with **Single User** or **Shared** access mode |
| `There is no current catalog set` | No `USE CATALOG` statement | Prefix with `catalog.schema.table` or run `USE CATALOG dev_catalog` |
| `DELTA_MISSING_TRANSACTION_LOG` | Table created externally without Delta log | Re-create with `USING DELTA` or run `CONVERT TO DELTA` |

---

### E. Quick Privilege Fix — Run All as Metastore Admin

If you keep hitting individual privilege errors, run all grants at once:

```sql
-- Grant all required metastore-level privileges to your user
GRANT CREATE EXTERNAL LOCATION    ON METASTORE TO `your-email@domain.com`;
GRANT CREATE CATALOG               ON METASTORE TO `your-email@domain.com`;
GRANT CREATE MANAGED STORAGE ON EXTERNAL LOCATION `metastore_root_location`
    TO `your-email@domain.com`;
GRANT READ FILES, WRITE FILES ON STORAGE CREDENTIAL `<your-storage-credential-name>`
    TO `your-email@domain.com`;
```

> **Simplest permanent fix:** Add your user as **Metastore Admin** in Account Console → Catalog → Admins tab. Metastore Admins have all privileges automatically.

---

## Quick Reference — Full SQL Script

```sql
-- ============================================================
-- COMPLETE UNITY CATALOG SETUP — RUN IN ORDER
-- ============================================================

-- 1. Storage credential (run once per access connector)
CREATE STORAGE CREDENTIAL `sc-adls-dev`
WITH AZURE_MANAGED_IDENTITY (
  CONNECTOR_ID = '/subscriptions/<SUB-ID>/resourceGroups/rg-databricks-uc/providers/Microsoft.Databricks/accessConnectors/adb-access-connector-dev'
);

-- 2. External locations
CREATE EXTERNAL LOCATION `ext-loc-bronze`
  URL 'abfss://bronze@stunitycatalogdev01.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL `sc-adls-dev`);

CREATE EXTERNAL LOCATION `ext-loc-silver`
  URL 'abfss://silver@stunitycatalogdev01.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL `sc-adls-dev`);

CREATE EXTERNAL LOCATION `ext-loc-gold`
  URL 'abfss://gold@stunitycatalogdev01.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL `sc-adls-dev`);

-- 3. Catalog and schemas
-- MANAGED LOCATION is required — without it managed tables go to Databricks Default Storage (not your ADLS)
CREATE CATALOG IF NOT EXISTS dev_catalog
MANAGED LOCATION 'abfss://metastore@valaxystadlsunitycatalog.dfs.core.windows.net/dev_catalog/';

USE CATALOG dev_catalog;

-- Schemas inherit the catalog MANAGED LOCATION automatically.
-- Explicit MANAGED LOCATION below overrides per layer if you want separate containers.
CREATE SCHEMA IF NOT EXISTS bronze
MANAGED LOCATION 'abfss://bronze@valaxystadlsunitycatalog.dfs.core.windows.net/managed/';

CREATE SCHEMA IF NOT EXISTS silver
MANAGED LOCATION 'abfss://silver@valaxystadlsunitycatalog.dfs.core.windows.net/managed/';

CREATE SCHEMA IF NOT EXISTS gold
MANAGED LOCATION 'abfss://gold@valaxystadlsunitycatalog.dfs.core.windows.net/managed/';

-- 4. Bronze managed table
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.customers (
  customer_id   BIGINT  NOT NULL,
  first_name    STRING  NOT NULL,
  last_name     STRING  NOT NULL,
  email         STRING,
  country       STRING,
  created_date  DATE,
  is_active     BOOLEAN DEFAULT true
) USING DELTA;

-- 5. Bronze external table
CREATE TABLE IF NOT EXISTS dev_catalog.bronze.sales_orders (
  order_id     BIGINT NOT NULL,
  customer_id  BIGINT NOT NULL,
  order_date   DATE,
  product_code STRING,
  quantity     INT,
  total_amount DECIMAL(10,2),
  status       STRING
) USING DELTA
LOCATION 'abfss://bronze@stunitycatalogdev01.dfs.core.windows.net/sales_orders/'
PARTITIONED BY (order_date);

-- 6. Silver layer
CREATE TABLE IF NOT EXISTS dev_catalog.silver.customer_orders
USING DELTA
LOCATION 'abfss://silver@stunitycatalogdev01.dfs.core.windows.net/customer_orders/'
AS SELECT
  c.customer_id,
  c.first_name || ' ' || c.last_name AS customer_name,
  c.country, o.order_id, o.order_date,
  o.product_code, o.quantity, o.total_amount, o.status
FROM dev_catalog.bronze.customers c
JOIN dev_catalog.bronze.sales_orders o ON c.customer_id = o.customer_id
WHERE c.is_active = true;

-- 7. Gold layer
CREATE TABLE IF NOT EXISTS dev_catalog.gold.customer_revenue_summary
USING DELTA
LOCATION 'abfss://gold@stunitycatalogdev01.dfs.core.windows.net/customer_revenue_summary/'
AS SELECT
  customer_id, customer_name, country,
  COUNT(order_id) AS total_orders,
  SUM(total_amount) AS total_revenue,
  MAX(order_date) AS last_order_date
FROM dev_catalog.silver.customer_orders
WHERE status = 'COMPLETED'
GROUP BY customer_id, customer_name, country;

-- 8. Verify
SHOW TABLES IN dev_catalog.bronze;
SHOW TABLES IN dev_catalog.silver;
SHOW TABLES IN dev_catalog.gold;
SELECT * FROM dev_catalog.gold.customer_revenue_summary;
```

---
