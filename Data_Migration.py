"""
Module Name: Data_Migration.py

Description:
This is a Low-Code module that provides data transfer between the following source and target:
1.) Data Source (any kind)     (DS) => (LZ) External Data Landing Zone
2.) External Data Landing Zone (LZ) => (RZ) Raw Zone
3.) Raw Zone                   (RZ) => (BL) Bronze Layer
4.) Bronze Layer               (BL) => (SL) Silver Layer
5.) Silver Layer               (SL) => (GL) Gold Layer
6.) Gold Layer                 (GL) => (DZ) Destination Zone [SQL Server]

Features:
* File Logging
* Test Data Read
* Catalog Writing
* Audit Row Trails
* Data Quality Check
* Isolated Test Data
* Datadog API Logging
* Data Integrity Check
* Display Output Table
* Test Error Simulation
* Agnostic source and target
* Debugging with Tracking Point
* Parameter-ready for ADF Pipelines
* Automatic Assignment of Target Layer
* Eliminates multiple notebooks for ADF Pipelines
* No repeated functions across different data layers
* Configuration-embedded settings (not Code-embedded)
* Automatic Renaming, Deletion, Sorting and setting of Data Types

Debugging:
self.Debug("<Debug Value>")

Tips:
* If you get "504 Deadline Exceeded" error in GA4 API, 
  lower the Fetch Limit or if possible limit the number of columns

Author: Erwin Bernard Talento
Email: etalento.contractor@xxx.xxx
"""

class Data_Migration (Data_Specs, Shared_MainLib):

    def __init__ (
            self,
            arg_Datahouse = None,
            arg_Repository = None,
            arg_DebugMode = False,
            arg_TestMode =  False
        ):
        self.InitSuccess = True
        try:
            # ======== Load Shared Library:
            super().__init__()
            # save the Exception of SharedLib if existing
            vSharedLibException = self.clean_Exception()
            self._DataAttributes = ["Metrics", "Dimensions"]
            # ======== Transfer the traits to Common Library
            if arg_Datahouse is not None:
                self.App["Metadata"]["Datahouse"] = arg_Datahouse
            if arg_Repository is not None:
                self.App["Metadata"]["Repository"] = arg_Repository
            self.AppConfig["Main"]["Switchboard"]["DebugMode"] = arg_DebugMode
            self.AppConfig["Main"]["Switchboard"]["TestMode"] = arg_TestMode
            self.AppLib["Metadata"]["Datahouse"] = self.App["Metadata"]["Datahouse"]
            self.AppLib["Metadata"]["Repository"] = self.App["Metadata"]["Repository"]
            self.LibConfig["Main"]["Switchboard"]["TestMode"] = self.AppConfig["Main"]["Switchboard"]["TestMode"]
            self.LibConfig["Main"]["Switchboard"]["DebugMode"] = self.AppConfig["Main"]["Switchboard"]["DebugMode"]
            self.LibConfig["Main"]["Switchboard"]["SilentMode"] = self.AppConfig["Main"]["Switchboard"]["SilentMode"]
            # Check if Shared Library's Initialization has an error
            if self.InitSuccess is False:
                raise Exception(f"Shared Library Initialization Failed: {vSharedLibException}")
        except Exception as ExceptionError:
            self._Exception = ExceptionError
            self.InitSuccess = False
        return

    def Start (
            self,
            arg_MediaType,
            arg_MediaClass,
            arg_Source,
            arg_Target = 'Auto'
        ):
        TraceMsg = None
        vStartSuccess = False
        vCallerUnknown = "Unknown"
        try:
            # ======== This will halt the error info and needs to be handled first
            if "Main" in self.AppConfig and "Label" in self.AppConfig["Main"] and arg_Source in self.AppConfig["Main"]["Data"]["Label"]:
                vCallerSource = self.AppConfig["Main"]["Data"]["Label"][arg_Source]
            else:
                vCallerSource = vCallerUnknown
            if "Main" in self.AppConfig and "Label" in self.AppConfig["Main"] and arg_Target in self.AppConfig["Main"]["Data"]["Label"]:
                vCallerTarget = self.AppConfig["Main"]["Data"]["Label"][arg_Target]
            else:
                vCallerTarget = vCallerUnknown
            vCompleteCaller = f'{arg_MediaType}\\{arg_MediaClass} {self.App["Request"]["Caller"]} - {vCallerSource} ::> {vCallerTarget}'
        except Exception:
            vCompleteCaller =  vCallerUnknown
        # ======== Import the traceback module for error handling
        import traceback
        try:
            self._TotalRows = 0
            self._AddedColumns = 0
            self._PurgedColumns = 0
            self._UpdatedColumns = 0
            self._RenamedColumns = 0
            is_ErrorSimulated = False
            is_MigrationError = False
            self._MediaType = arg_MediaType
            self._MediaClass = arg_MediaClass
            self._Source = arg_Source
            self._Target = arg_Target
            from datetime import (
                datetime, 
                timedelta
            )
            # ======== Check if both App & Library initialization are successful
            if self.InitSuccess is False:
                raise MigrationError(
                        self.show_ErrorMsg("FailedAppInit", self.clean_Exception())
                    )

            vStartDT = datetime.now()
            # ======== Set Response Header:
            if self.App["Request"]["ADF"]["PipelineCaller"] is not None:
                self.App["Request"]["Caller"] = "Pipeline"
            self.App["Response"]["Header"]["App"]["Statistics"]["Started"] = self.get_PHdatetime(vStartDT)
            self.App["Response"]["Header"]["App"]["Name"] = self.App["Metadata"]["Name"]
            self.App["Response"]["Header"]["App"]["Version"] = self.App["Metadata"]["Version"]
            self.App["Response"]["Header"]["App"]["Datahouse"] = self.App["Metadata"]["Datahouse"]
            self.App["Response"]["Header"]["App"]["Repository"] = self.App["Metadata"]["Repository"]
            self.App["Response"]["Header"]["App"]["Caller"]["Pipeline"] = self.App["Request"]["ADF"]
            self.App["Response"]["Header"]["App"]["Caller"]["Origin"] = self.App["Request"]["Caller"]
            self.App["Response"]["Header"]["App"]["Caller"]["ConfigFile"] = self.App["Request"]["ConfigFile"]
            self.App["Response"]["Header"]["App"]["Environment"] = self._Environment
            self.App["Response"]["Header"]["App"]["Mode"] = {
                "Test" : self.AppConfig["Main"]["Switchboard"]["TestMode"],
                "Debug" : self.AppConfig["Main"]["Switchboard"]["DebugMode"]
            }
            # ======== Get the notebook path
            vAppPath = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
            if vAppPath:
                self.App["Response"]["Header"]["App"]["File"] = vAppPath
            else:
                self.App["Response"]["Header"]["App"]["File"] = None
            # ======== Get the username of the person who run this
            vRunner = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
            if vRunner:
                self.App["Response"]["Header"]["App"]["Runner"] = vRunner
            else:
                self.App["Response"]["Header"]["App"]["Runner"] = None
            # ======== Is Payload passed by a caller file?
            # If no payload, set blank object to ADF:Payload parameter
            if self.App["Request"]["ADF"]["Payload"]:
                if all(Keyname in self.App["Request"]["ADF"]["Payload"] for Keyname in ["Scope", "Config", "Key"]):
                    # ======== Check if both of the parameters are Falsy
                    if not self.App["Request"]["ADF"]["Payload"]["Scope"] and not self.App["Request"]["ADF"]["Payload"]["Key"]:
                        vCallerConfig = self.App["Request"]["ADF"]["Payload"]["Config"].keys()
                        # ======== Configuration-specific
                        match vCallerConfig:
                            case "test":
                                pass

                            case _:
                                pass
                        
                    elif self.App["Request"]["ADF"]["Payload"]["Scope"] and self.App["Request"]["ADF"]["Payload"]["Key"]:
                        self.reset_Status("FailedDecryption")
                        retVal = self.decrypt_Data(
                            self.App["Request"]["ADF"]["Payload"]["Scope"], 
                            self.App["Request"]["ADF"]["Payload"]["Config"], 
                            self.App["Request"]["ADF"]["Payload"]["Key"]
                        )
                        if self._ReturnStatus is True:
                            self.AppConfig = self._ReturnValue

            # ======== Retrieve keys for the current MediaType class
            vTypeKeys = list(self.AppConfig["Media"].keys())
            # ======== Join the class keys into a string
            voptionType = ", ".join(vTypeKeys)
            # ======== Check if the provided MediaType is valid
            if self._MediaType not in vTypeKeys:
                raise MigrationError(
                    self.show_ErrorMsg("SyntaxMediaType", self._MediaType, voptionType)
                )

            # ======== Retrieve keys for the current MediaType class
            vClassKeys = self.AppConfig["Media"][self._MediaType]["Class"].keys()
            # ======== Join the class keys into a string
            voptionType = ", ".join(vClassKeys)
            # ======== Check if the provided MediaClass is valid
            if self._MediaClass not in vClassKeys:
                raise MigrationError(
                    self.show_ErrorMsg("SyntaxMediaClass", self._MediaClass, voptionType)
                )

            # ======== Join the source options into a string
            voptionSource = ", ".join(self.AppConfig["Main"]["Data"]["Source"])
            # ======== Check if the provided Source is valid
            if self._Source not in self.AppConfig["Main"]["Data"]["Source"]:
                raise MigrationError(
                    self.show_ErrorMsg("SyntaxSource", self._Source, voptionSource)
                )

            # ======== Check if target is not Auto and debug is enabled
            if self._Target != "Auto":
                # ======== Join the target options into a string
                voptionTarget = ", ".join(self.AppConfig["Main"]["Data"]["Target"])
                # ======== Check if the provided Target is valid
                if self._Target not in self.AppConfig["Main"]["Data"]["Target"]:
                    raise MigrationError(
                        self.show_ErrorMsg("SyntaxTarget", self._Target, voptionTarget)
                    )
                # ======== Check if Source and Target are the same
                if self._Source == self._Target:
                    # ======== They are the same
                    raise MigrationError(
                        self.show_ErrorMsg("SyntaxLineage")
                    )
                vCompleteCaller = f'{arg_MediaType}\\{arg_MediaClass} {self.App["Request"]["Caller"]} - {self.AppConfig["Main"]["Data"]["Label"][self._Source]} ::> {self.AppConfig["Main"]["Data"]["Label"][self._Target]}'
            # ======== Target is Automatically-Assigned
            else:
                # ======== Select correct target
                self._Target = self.AppConfig["Main"]["Data"]["Lineage"][self._Source]
                vCallerTarget = self.AppConfig["Main"]["Data"]["Label"][self._Target]
                vCallerSource = self.AppConfig["Main"]["Data"]["Label"][self._Source]
                vCompleteCaller = f'{arg_MediaType}\\{arg_MediaClass} {self.App["Request"]["Caller"]} - {vCallerSource} ::> {vCallerTarget}'
            from pyspark.sql.functions import (
                col,
                lit,
                sha2,
                split,
                count,
                concat,
                concat_ws,
                substring,
                date_format,
                collect_list,
                regexp_extract,
                regexp_replace,
                sum as spark_sum,
                current_timestamp
            )
            from pyspark.sql import (
                Row,
                SparkSession
            )
            from pyspark.sql.types import (
                LongType,
                TimestampType
            )
            # ======== Show the module name, version, datahouse and its environment
            self.show_Info("Module", f'{self.App["Metadata"]["Name"]} v{self.App["Metadata"]["Version"]}', "blue")
            self.show_Info("Datahouse", f'{self.App["Metadata"]["Datahouse"]}', "blue")
            self.show_Info("Environment", f'{self._Environment}\r\n', "blue")
            vAppConfigMediaClass = self.AppConfig["Media"][self._MediaType]["Class"][self._MediaClass]
            vAppConfigStageSource = self.AppConfig["Media"][self._MediaType]["Stage"][self._Source]
            vSchemaTransformation = vAppConfigMediaClass["Schema"]["Transformation"]
            # ======== Check if debug mode is enabled
            if self.AppConfig["Main"]["Switchboard"]["DebugMode"] is True:
                # ======== Set debug directory path
                if "Directory" in self.AppConfig["Media"][self._MediaType]["Common"]:
                    vPath_ParentDir = self.AppConfig["Media"][self._MediaType]["Common"]["Directory"]["Debug"]
                else:
                    vPath_ParentDir = vAppConfigMediaClass["Directory"]["Debug"]
                # ======== Simulate a MigrationError
                if self.AppConfig["Main"]["Switchboard"]["ExceptionApp"] is True:
                    is_ErrorSimulated = True
                    raise MigrationError(
                        self.show_ErrorMsg("TestError")
                    )
                # ======== Simulate an Exception Error
                if self.AppConfig["Main"]["Switchboard"]["ExceptionAll"] is True:
                    is_ErrorSimulated = True
                    # ======== Divide by Zero
                    dividebyzero = 8 / 0
            else:
                # ======== Set live directory path
                if "Directory" in self.AppConfig["Media"][self._MediaType]["Common"]:
                    vPath_ParentDir = self.AppConfig["Media"][self._MediaType]["Common"]["Directory"]["Live"]
                else:
                    vPath_ParentDir = vAppConfigMediaClass["Directory"]["Live"]
                self.App["Response"]["Header"]["App"]["Caller"]["MediaParent"] = vPath_ParentDir
            # ======== Set the Response Argument
            self.App["Response"]["Header"]["App"]["Caller"]["MediaType"] = self._MediaType
            self.App["Response"]["Header"]["App"]["Caller"]["MediaClass"] = self._MediaClass
            self.App["Response"]["Header"]["App"]["Caller"]["Source"] = self._Source
            self.App["Response"]["Header"]["App"]["Caller"]["Target"] = self._Target
            # ======== Start a Spark session
            spark = SparkSession.builder.appName("Transfer_Data").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
            """
            ╔═══════════════════════════════════════════════╗
            ║ PROPER USAGE of TRANSFORMATION TABLE & LAYERS ║
            ╚═══════════════════════════════════════════════╝
            "Media" : {
                "<MediaType>" : {
                    "Class" : {
                        "<MediaClass>" : {
                            "Schema" : {
                                "Transformation" : {
                                    "Table" : {
                                        "Name" : "<Customized Table Name>",
                                        "Layers" : { # Applicable to these layers
                                            "BL", "SL", "GL", "DZ"
                                        }
                                    }
                                }
            """
            # Check if Table name needs to be customized
            if "Table" in vSchemaTransformation and \
                "Name" in vSchemaTransformation["Table"] and \
                "Layers" in vSchemaTransformation["Table"]:
                # ======== Get Customized Name for the Table
                if self._Source in vSchemaTransformation["Table"]["Layers"]:
                    vMediaClassSource = vSchemaTransformation["Table"]["Name"]
                else:
                    vMediaClassSource = self._MediaClass
                if self._Target in vSchemaTransformation["Table"]["Layers"]:
                    vMediaClassTarget = vSchemaTransformation["Table"]["Name"]
                else:
                    vMediaClassTarget = self._MediaClass
            else:
                vMediaClassSource = vMediaClassTarget = self._MediaClass
            # ======== Convert Media Class to valid DB Name
            vTargetName = vMediaClassTarget
            if '/' in vTargetName:
                vTargetName = vTargetName.replace('/', '_')
            # ======== Read Source
            match vAppConfigStageSource["Provider"]["Type"]:
                # case "API/FB":
                    # ╔════════════════════════════════════════════╗
                    # ║ Data Source (Media Type) & other use cases ║
                    # ╚════════════════════════════════════════════╝
                    # %run ./Source/FB/FB_API_Account12345678
                    # FB = Facebook_API()
                    # FB.Start()
                    # if self._ReturnStatus is True:
                    #     vAssortedData = self._ReturnValue

                case "API/GA4":
                    try:
                        %pip show google-analytics-data > /dev/null 2>&1
                    except Exception as ExceptionError:
                        self.show_Info("Installing", "Google Analytics")
                        %pip install --upgrade google-analytics-data > /dev/null 2>&1
                        %pip install --upgrade google-auth > /dev/null 2>&1
                        %pip install --upgrade google-auth-oauthlib > /dev/null 2>&1
                        %pip install --upgrade google-auth-httplib2 > /dev/null 2>&1
                    # ======== Check if StartDate is not set
                    if vAppConfigMediaClass["API"]["StartDate"] is None:
                        # ======== Incremental
                        vTargetAPI = self.AppConfig["Main"]["Data"]["Lineage"][self._Source]
                        # Read LZ's MAX(date) to LZDate+1
                        # Check if LZDate is not the same with yesterday & beyond
                        # Set end date to LZ's MAX(date)
                        # Etcetera
                    # ======== Check if EndDate is not set
                    if vAppConfigMediaClass["API"]["EndDate"] is None:
                        # ======== Set end date for today
                        vEndDate = datetime.now().strftime('%Y-%m-%d')
                    else:
                        # ======== Use provided end date
                        vEndDate = vAppConfigMediaClass["API"]["EndDate"]
                    # ======== Populate GA4 credentials from Vault
                    vSourceAccountAPI = vAppConfigStageSource["Provider"]["Account"]
                    vSourceSecretAPI = vAppConfigStageSource["Secret"]
                    vX509 = dbutils.secrets.get(
                        scope=vSourceSecretAPI["URLx509"]["Scope"], 
                        key=vSourceSecretAPI["URLx509"]["Key"]
                    )
                    vCredentials = {
                        "type": vSourceAccountAPI["Type"], 
                        "auth_uri": vSourceAccountAPI["URL_Auth"], 
                        "token_uri": vSourceAccountAPI["URL_Token"], 
                        "auth_provider_x509_cert_url": vX509, 
                        "project_id": dbutils.secrets.get(
                                scope=vSourceSecretAPI["ProjectID"]["Scope"], 
                                key=vSourceSecretAPI["ProjectID"]["Key"]
                            ), 
                        "client_email": dbutils.secrets.get(
                                scope=vSourceSecretAPI["ClientEmail"]["Scope"], 
                                key=vSourceSecretAPI["ClientEmail"]["Key"]
                            ), 
                        "client_id": dbutils.secrets.get(
                                scope=vSourceSecretAPI["ClientID"]["Scope"], 
                                key=vSourceSecretAPI["ClientID"]["Key"]
                            ), 
                        "client_x509_cert_url": vX509,
                        "private_key_id": dbutils.secrets.get(
                                scope=vSourceSecretAPI["PrivateKeyID"]["Scope"], 
                                key=vSourceSecretAPI["PrivateKeyID"]["Key"]
                            ), 
                        "private_key" : dbutils.secrets.get(
                                scope=vSourceSecretAPI["PrivateKey"]["Scope"], 
                                key=vSourceSecretAPI["PrivateKey"]["Key"]
                            ).replace("\\n", "\n")
                    }
                    """
                    ╔════════════════════════════╗
                    ║ PROPER USAGE of API FILTER ║
                    ╚════════════════════════════╝
                    "Media" : {
                        "<MediaType>" : {
                            "Class" : {
                                "<MediaClass>" : {
                                    "API" : {
                                        "PropertyID" : "1234567890",
                                        "StartDate" : "1900-01-01",
                                        "EndDate" : None,
                                        "Fetch_Offset" : 0,
                                        "Fetch_Limit" : 100000,
                                        "Filter" : {
                                            "DimensionInList" : {
                                                "eventName" : {
                                                    "org_file_download",
                                                    "file_download"
                                                }
                                            }
                                        }
                    """
                    if "Filter" in vAppConfigMediaClass["API"] and vAppConfigMediaClass["API"]["Filter"]:
                        vFilterAPI = vAppConfigMediaClass["API"]["Filter"]
                    else:
                        vFilterAPI = None
                    # ======== Pull GA4 data
                    self.pull_GA4API(
                            vCredentials,
                            vAppConfigMediaClass["API"]["PropertyID"],
                            vAppConfigMediaClass["API"]["StartDate"],
                            vEndDate,
                            vAppConfigMediaClass["Schema"]["Dimensions"].keys(),
                            vAppConfigMediaClass["Schema"]["Metrics"].keys(),
                            vAppConfigMediaClass["API"]["Fetch_Offset"],
                            vAppConfigMediaClass["API"]["Fetch_Limit"],
                            vFilterAPI
                        )
                    # ======== Define source location for API
                    # e.g., Google/GA4/DS/GA4-Page_Daily-DS
                    vSourceLoc = self.get_MediaPath(
                        vPath_ParentDir,
                        self._MediaType,
                        self._MediaClass,
                        self._Source
                    )
                    # ======== End of GA4 Block

                case "Azure/Blob":
                    # ======== Get full source location path
                    # e.g., Google/GA4/LZ/GA4-Page_Daily-LZ
                    vSourceLoc = self.get_MediaPath(
                        vPath_ParentDir,
                        self._MediaType,
                        vMediaClassSource,
                        self._Source,
                        vAppConfigStageSource["Provider"]["SubDir"]["Path"],
                        vAppConfigStageSource["Provider"]["SubDir"]["Extension"]
                    )
                    # ======== Read Blob
                    self.read_AzureBlob(
                            vAppConfigStageSource["Secret"]["StorageAccount"]["Scope"],
                            vAppConfigStageSource["Secret"]["StorageAccount"]["Key"],
                            vAppConfigStageSource["Secret"]["StorageContainer"]["Scope"],
                            vAppConfigStageSource["Secret"]["StorageContainer"]["Key"],
                            vSourceLoc,
                            vAppConfigStageSource["Provider"]
                        )

                case _:
                    self.reset_Status("InvalidSourceType")

            # ======== Check if read operation was successful
            if self._ReturnStatus is False:
                # ======== Failed Source Reading
                self.show_Info("Source", f"{vSourceLoc} ::> Failed", "red")
                raise MigrationError(
                    self.show_ErrorMsg()
                )
            else:
                # ======== Successful Source Reading
                self.show_Info("Source", f"{vSourceLoc} ::> Successful")
                # ======== Retrieve the returned data
                vAssortedData = self._ReturnValue
                if self._TotalRows == 0:
                    self._TotalRows = vAssortedData.count()
                self.App["Response"]["Schema"]["Rows"]["GrossTotal"] = self._TotalRows
                # ======== Do we need to run Data Quality Checks?
                if self._Target != "Read" and \
                    self.AppConfig["Main"]["Switchboard"]["QualityCheck"] is True and \
                    vAppConfigMediaClass["Schema"]["Quality"]["Duplicates"]["Remove"] is True and \
                    vAppConfigMediaClass["Schema"]["Quality"]["Duplicates"]["Fields"] and \
                    self._TotalRows != 0:
                    # ╔════════════════════════════════════════════╗
                    # ║ Data Quality Check - Remove Duplicate Rows ║
                    # ╚════════════════════════════════════════════╝
                    vDupliFields = vAppConfigMediaClass["Schema"]["Quality"]["Duplicates"]["Fields"]
                    vIncludedColumns = [col for col in vAssortedData.columns if not col.startswith("Row_")]
                    vDuplicateData = vAssortedData.select(*vIncludedColumns)
                    """
                    ╔═════════════════════════════════════════╗
                    ║ PROPER USAGE of DROPPING DUPLICATE ROWS ║
                    ╚═════════════════════════════════════════╝
                    "Media" : {
                        "<MediaType>" : {
                            "Class" : {
                                "<MediaClass>" : {
                                    "Schema" : {
                                        "Quality" : {
                                            "Duplicates" : {
                                                "Remove" : False,
                                                "Fields" : {"*"}
                                                
                                                <OR>

                                                "Fields" : {
                                                    "Column1", 
                                                    "Column2", 
                                                    "etc."
                                                }
                    """
                    if "*" in vDupliFields:
                        vDuplicateData = vDuplicateData.dropDuplicates()
                    else:
                        vDupliList = list(vDupliFields)
                        vDuplicateData = vDuplicateData.dropDuplicates(vDupliList)
                    vNetTotalRows = vDuplicateData.count()
                    vDuplicateRows = self._TotalRows - vNetTotalRows
                    # ======== Check if any duplicates were found
                    if "*" in vDupliFields:
                        if vDuplicateRows == 0:
                            self.show_Info("QC Check", f"No Rows Removed for All Columns ::> Zero (0) Duplicate Rows Found", "yellow")
                        else:
                            self.show_Info("QC Check", f"Removed Duplicate Rows for All Columns ::> {vDuplicateRows}", "yellow")
                            vAssortedData = vDuplicateData
                    else:
                        vDupliCommaList = ", ".join(vDupliList)
                        if vDuplicateRows == 0:
                            self.show_Info("QC Check", f"No Rows Removed for [{vDupliCommaList}] ::> Zero (0) Duplicate Rows Found", "yellow")
                        else:
                            self.show_Info("QC Check", f"Removed Duplicate Rows for [{vDupliCommaList}] ::> {vDuplicateRows}", "yellow")
                            vAssortedData = vDuplicateData
                    self.App["Response"]["Schema"]["Rows"]["NetTotal"] = vNetTotalRows
                    self.App["Response"]["Schema"]["Rows"]["Duplicates"] = vDuplicateRows
                else:
                    self.App["Response"]["Schema"]["Rows"]["NetTotal"] = self._TotalRows
                    self.App["Response"]["Schema"]["Rows"]["Duplicates"] = 0
                if self._Target != "Read" and self.AppConfig["Main"]["Switchboard"]["QualityCheck"] is True and self._TotalRows != 0:
                    """
                    ╔═══════════════════════════════════════════════════════════════════════════════╗
                    ║ PROPER USAGE of QUALITY CHECK, CASTING DATA TYPES, RENAMING & DROPPING FIELDS ║
                    ╚═══════════════════════════════════════════════════════════════════════════════╝
                    "Media" : {
                        "<MediaType>" : {
                            "Class" : {
                                "<MediaClass>" : {
                                    "Schema" : {
                                        "Dimensions" : {
                                            "countryId" : {
                                                "Type" : "string",
                                                "Drop" : False,
                                                "Rename" : "RK-MediumCountry",
                                                "Quality" : {
                                                    "Null", "Blank", "Unique", "UserDefined:alphanumeric"
                                                }
                                            },
                                        },
                                        "Metrics" : {
                                            "sessions" : {
                                                "Type" : "integer",
                                                "Drop" : False,
                                                "Rename" : "Sessions",
                                                "Quality" : {
                                                    "Null", "Blank", "Unique", "UserDefined:alphanumeric"
                                                }
                                            },
                                        }
                    """
                    # ======== Scour each attribute
                    for vAttribute in self._DataAttributes:
                        # ======== Scour each column
                        # The original field name will be changed in the SL & GL data layers
                        for vColumn, vColumnSpecs in vAppConfigMediaClass["Schema"][vAttribute].items():
                            # ======== Check if column exists
                            if self.is_ColumnExists(vAssortedData, vColumn) is True:
                                # ======== Column is existing
                                if "Quality" in vColumnSpecs:
                                    for vQualityCheck in vColumnSpecs["Quality"]:
                                        match vQualityCheck:
                                            case "Null":
                                                # ╔═══════════════════════════╗
                                                # ║ Data Quality Check - Null ║
                                                # ╚═══════════════════════════╝
                                                vNullCount = vAssortedData.filter(col(vColumn).isNull()).count()
                                                vNullPercentage = (vNullCount / self._TotalRows) * 100
                                                self.App["Response"]["Schema"]["Quality"]["Null"] = {
                                                    vColumn : {
                                                        "Count" : vNullCount,
                                                        "Percentage" : vNullPercentage
                                                    }
                                                }
                                                if self.AppConfig["Main"]["Switchboard"]["DisplayTable"] is False:
                                                    self.show_Info("QC Check", f"Field [{vColumn}] ::> Null Count = {vNullCount} ({vNullPercentage:.2f}%)", "yellow")
                                                            
                                            case "Blank":
                                                # ╔════════════════════════════╗
                                                # ║ Data Quality Check - Blank ║
                                                # ╚════════════════════════════╝
                                                vWhitespaceCount = vAssortedData.filter(col(vColumn).rlike(r"^\s+$")).count()
                                                vBlankCount = vAssortedData.filter(col(vColumn) == "").count() + vWhitespaceCount
                                                vBlankPercentage = (vBlankCount / self._TotalRows) * 100
                                                self.App["Response"]["Schema"]["Quality"]["Blank"] = {
                                                    vColumn : {
                                                        "Count" : vBlankCount,
                                                        "Percentage" : vBlankPercentage
                                                    }
                                                }
                                                if self.AppConfig["Main"]["Switchboard"]["DisplayTable"] is False:
                                                    self.show_Info("QC Check", f"Field [{vColumn}] ::> Blank/Whitespace Count = {vBlankCount} ({vBlankPercentage:.2f}%)", "yellow")
                                                
                                            case "Unique":
                                                # ╔═══════════════════════════════╗
                                                # ║ Data Quality Check - Distinct ║
                                                # ╚═══════════════════════════════╝
                                                vDistinctCount = vAssortedData.select(vColumn).distinct().count()
                                                vDistinctPercentage = (vDistinctCount / self._TotalRows) * 100
                                                self.App["Response"]["Schema"]["Quality"]["Unique"] = {
                                                    vColumn : {
                                                        "Count" : vDistinctCount,
                                                        "Percentage" : vDistinctPercentage
                                                    }
                                                }
                                                if self.AppConfig["Main"]["Switchboard"]["DisplayTable"] is False:
                                                    self.show_Info("QC Check", f"Field [{vColumn}] ::> Unique Count = {vDistinctCount} ({vDistinctPercentage:.2f}%)", "yellow")
                                                vDuplicateCount = self._TotalRows - vDistinctCount
                                                vDuplicatePercentage = (vDuplicateCount / self._TotalRows) * 100
                                                self.App["Response"]["Schema"]["Quality"]["Duplicate"] = {
                                                    vColumn : {
                                                        "Count" : vDuplicateCount,
                                                        "Percentage" : vDuplicatePercentage
                                                    }
                                                }
                                                if self.AppConfig["Main"]["Switchboard"]["DisplayTable"] is False:
                                                    self.show_Info("QC Check", f"Field [{vColumn}] ::> Duplicate Count = {vDuplicateCount} ({vDuplicatePercentage:.2f}%)", "yellow")

                                            case _:
                                                if ':' in vQualityCheck:
                                                    # ╔═══════════════════════════════════╗
                                                    # ║ Data Quality Check - User Defined ║
                                                    # ╚═══════════════════════════════════╝
                                                    vDefinedName, vDEName = vQualityCheck.split(':')
                                                    if vDEName in self.AppConfig["Main"]["Data"]["Expressions"] and vDefinedName == "UserDefined":
                                                        vDefinedExpressions = self.AppConfig["Main"]["Data"]["Expressions"][vDEName]
                                                        vDefinedCount = vAssortedData.filter(col(vColumn).rlike(vDefinedExpressions)).count()
                                                        vDefinedPercentage = (vDefinedCount / self._TotalRows) * 100
                                                        self.App["Response"]["Schema"]["Quality"]["UserDefined"] = {
                                                            vDEName : {
                                                                vColumn : {
                                                                    "Count" : vDefinedCount,
                                                                    "Percentage" : vDefinedPercentage
                                                                }
                                                            }
                                                        }
                                                        if self.AppConfig["Main"]["Switchboard"]["DisplayTable"] is False:
                                                            self.show_Info("QC Check", f"Field [{vColumn}] ::> User-Defined [{vDEName}] Count = {vDefinedCount} ({vDefinedPercentage:.2f}%)", "yellow")
                # ======== Do we need to run the integrity check?
                if self.AppConfig["Main"]["Switchboard"]["IntegrityCheck"] is True and self._TotalRows != 0:
                    # ╔══════════════════════╗
                    # ║ Data Integrity Check ║
                    # ╚══════════════════════╝
                    # This should be placed before the transformation to eliminate
                    # the false warning that Raw Zone is not equal to Bronze Layer
                    self.show_Info("Integrity", "Generating data integrity checksum")
                    # ======== Concatenate all columns into a single string for each row
                    vIncludedColumns = [col for col in vAssortedData.columns if not col.startswith("Row_")]
                    vAssortedData = vAssortedData.withColumn(
                            "Row_Concat", concat_ws("||", *vIncludedColumns)
                        )
                    # ======== Sort to get the consistent checksum
                    vAssortedData = vAssortedData.orderBy(
                            vAssortedData["Row_Concat"].asc()
                        )
                    # ======== Compute SHA-256 hash for each row
                    vAssortedData = vAssortedData.withColumn(
                            "Row_Checksum", sha2(col("Row_Concat"), 256)
                        )
                    # ======== Aggregate all row hashes into a single checksum
                    vChecksumConcat = vAssortedData.agg(
                            concat_ws("", collect_list("Row_Checksum")).alias("ConcatenatedChecksums")
                        ).collect()[0]["ConcatenatedChecksums"]
                    import hashlib
                    vDataChecksum = hashlib.sha256(vChecksumConcat.encode()).hexdigest()
                    # ======== Delete temporary column
                    vAssortedData = vAssortedData.drop("Row_Concat")
                    self.show_Info("Checksum", vDataChecksum, "cyan")
                    self.App["Response"]["Schema"]["Integrity"]["Checksum"] = vDataChecksum
                    self.App["Response"]["Schema"]["Integrity"]["Hash"] = "SHA-256"
                else:
                    self.App["Response"]["Schema"]["Integrity"]["Checksum"] = None
                # ======== Check if we need to transform the Source
                if self.AppConfig["Main"]["Data"]["Transformer"] == self._Source and self._Target != "Read":
                    # ======== Start of Transformation
                    # ╔═══════════════════════════════════╗
                    # ║ Transformation Area - Filter Data ║
                    # ╚═══════════════════════════════════╝
                    if "Filter" in vSchemaTransformation and vSchemaTransformation["Filter"]:
                        """
                        ╔═══════════════════════════════╗
                        ║ PROPER USAGE of SCHEMA FILTER ║
                        ╚═══════════════════════════════╝
                        "Media" : {
                            "<MediaType>" : {
                                "Class" : {
                                    "<MediaClass>" : {
                                        "Schema" : {
                                            "Transformation" : {
                                                "Filter" : {
                                                    "OR" : {
                                                        "!=" : {
                                                            "Age" : {
                                                                20,
                                                                40
                                                            }
                                                        }
                                                    },
                                                    "AND" : {
                                                        "==" : {
                                                            "City" : {
                                                                "Lipa City",
                                                                "Batangas City"
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                        """
                        vExpression = None
                        vDisplayPath = None
                        vDisplayValue = None
                        for vLogicalOperand, vComparisons in vSchemaTransformation["Filter"].items():
                            for vComparisonOperand, vConditions in vComparisons.items():
                                for vColName, vCondition in vConditions.items():
                                    if self.is_ColumnExists(vAssortedData, vColName) is True:
                                        for vColValue in vCondition:
                                            is_ValidOperand = True
                                            match vComparisonOperand:
                                                case "==":
                                                    if vColValue is None or vColValue == "Null":
                                                        vCondition = (col(vColName).isNull())
                                                    else:
                                                        vCondition = (col(vColName) == vColValue)

                                                case "!=":
                                                    if vColValue is None or vColValue == "Null":
                                                        vCondition = (col(vColName).isNotNull())
                                                    else:
                                                        vCondition = (col(vColName) != vColValue)

                                                case ">":
                                                    if vColValue == "":
                                                        vColValue = 0
                                                    vCondition = (col(vColName) > vColValue)

                                                case ">=":
                                                    if vColValue == "":
                                                        vColValue = 0
                                                    vCondition = (col(vColName) >= vColValue)

                                                case "<":
                                                    if vColValue == "":
                                                        vColValue = 0
                                                    vCondition = (col(vColName) < vColValue)

                                                case "<=":
                                                    if vColValue == "":
                                                        vColValue = 0
                                                    vCondition = (col(vColName) <= vColValue)

                                                case _:
                                                    is_ValidOperand = False

                                            if vColValue is None or vColValue == "Null":
                                                vDisplayValue = "Null"
                                            elif vColValue == "":
                                                vDisplayValue = "Blank"
                                            else:
                                                vDisplayValue = vColValue
                                            vDisplayExpression = f"{vColName} {vComparisonOperand} {vDisplayValue}"
                                            if vDisplayPath is None:
                                                vDisplayPath = vDisplayExpression
                                            else:
                                                if vLogicalOperand == "OR":
                                                    vDisplayPath = f"{vDisplayPath} OR {vDisplayExpression}"
                                                elif vLogicalOperand == "AND":
                                                    vDisplayPath = f"{vDisplayPath} AND {vDisplayExpression}"
                                            if vExpression is None:
                                                vExpression = vCondition
                                            else:
                                                if vLogicalOperand == "OR":
                                                    vExpression = vExpression | vCondition
                                                elif vLogicalOperand == "AND":
                                                    vExpression = vExpression & vCondition
                        vOldTotalRows = self._TotalRows
                        vAssortedData = vAssortedData.filter(vExpression)
                        vNewTotalRows = vAssortedData.count()
                        vFilteredRows = vOldTotalRows - vNewTotalRows
                        self.App["Response"]["Schema"]["Rows"]["NetTotal"] = vNewTotalRows
                        self.App["Response"]["Schema"]["Rows"]["Filtered"] = vFilteredRows
                        self.show_Info("Filter", f" WHERE {vDisplayPath} ::> Successful", "yellow")
                        self.show_Info("Filter", f" Removed Rows ::> {format(vFilteredRows, ',')}", "yellow")
                        self.show_Info("Filter", f" Net Total Rows ::> {format(vNewTotalRows, ',')}", "yellow")
                    # ╔══════════════════════════════════════╗
                    # ║ Transformation Area - Update Columns ║
                    # ╚══════════════════════════════════════╝
                    if self._MediaType == "GA4":
                        vColumn = "date"
                        if self.is_ColumnExists(vAssortedData, vColumn) is True:
                            # ======== Column existing, then Update Column to YYYY-MM-DD
                            vAssortedData = vAssortedData.withColumn(
                                "date",
                                concat_ws(
                                    "-",
                                    substring("date", 1, 4),  # Extract year
                                    substring("date", 5, 2),  # Extract month
                                    substring("date", 7, 2)   # Extract day
                                )
                            )
                            # ======== Is it successful?
                            if self.is_ColumnExists(vAssortedData, vColumn) is True:
                                if self._UpdatedColumns == 0:
                                    self.App["Response"]["Schema"]["Columns"]["Dimensions"]["Updated"] = set()
                                self._UpdatedColumns += 1
                                self.App["Response"]["Schema"]["Columns"]["Dimensions"]["Updated"].add(vColumn)
                                self.App["Response"]["Schema"]["Columns"]["Metrics"]["Updated"] = self._UpdatedColumns
                                self.show_Info("Updated Col", f"{vColumn} ::> Successful", "yellow")
                            else:
                                self.show_Info("Updated Col", f"{vColumn} ::> Failed", "red")
                        else:
                            # ======== Column is not existing
                            self.show_Info("Updated Col", f"{vColumn} ::> Failed (Not Existing)", "red")
                    # ╔══════════════════════════════════════╗
                    # ║ Transformation Area - Set Data Types ║
                    # ╚══════════════════════════════════════╝
                    # ======== Scour each attribute
                    for vAttribute in self._DataAttributes:
                        # ======== Scour each column
                        for vColumn, vColumnSpecs in vAppConfigMediaClass["Schema"][vAttribute].items():
                            # ======== Set Dimension Data Types based from the Configuration
                            vTypeSet = vColumnSpecs["Type"]
                            self.set_SparkType(vTypeSet)
                            if self._ReturnStatus is True:
                                vDataType = self._ReturnValue
                                if self.is_ColumnExists(vAssortedData, vColumn) is True:
                                    # ======== Dimension is existing
                                    vAssortedData = vAssortedData.withColumn(
                                            vColumn, col(vColumn).cast(vDataType)
                                        )
                                    self.show_Info("Set Type", f"[{vColumn}] ::> '{vTypeSet}' ::> Successful", "yellow")
                                else:
                                    self.show_Info("Set Type", f"[{vColumn}] ::> Not Existing", "red")
                    # ╔══════════════════════════════════════╗
                    # ║ Transformation Area - Adding Columns ║
                    # ╚══════════════════════════════════════╝
                    match self._MediaType:
                        case "GA4":
                            match self._MediaClass:
                                case "Page_Metrics":
                                    # ╔══════════════╗
                                    # ║  Add Column  ║
                                    # ╚══════════════╝
                                    vColumn = "Engagement_Rate"
                                    if self.is_ColumnExists(vAssortedData, vColumn) is True:
                                        # ======== Column is already existing
                                        self.show_Info("Added Col", f"{vColumn} ::> Failed (Existing)", "red")
                                    else:
                                        # ======== Column Not existing, then Add New Column
                                        vAssortedData = vAssortedData.withColumn(
                                            vColumn,
                                            (col("engagedSessions") / col("sessions")).cast("float")
                                        )
                                        # ======== Is it successful?
                                        if self.is_ColumnExists(vAssortedData, vColumn) is True:
                                            if self._AddedColumns == 0:
                                                self.App["Response"]["Schema"]["Columns"]["Dimensions"]["Added"] = set()
                                            self._AddedColumns += 1
                                            self.App["Response"]["Schema"]["Columns"]["Dimensions"]["Added"].add(vColumn)
                                            self.App["Response"]["Schema"]["Columns"]["Metrics"]["Added"] = self._AddedColumns
                                            self.show_Info("Added Col", f"{vColumn} ::> Successful", "yellow")
                                        else:
                                            self.show_Info("Added Col", f"{vColumn} ::> Failed", "red")

                                case "Demo_GA4":
                                    # ╔══════════════╗
                                    # ║  Add Column  ║
                                    # ╚══════════════╝
                                    vColumn = "Medium_Source-Traffic"
                                    if self.is_ColumnExists(vAssortedData, vColumn) is True:
                                        # ======== Column is already existing
                                        self.show_Info("Added Col", f"{vColumn} ::> Failed (Existing)", "red")
                                    else:
                                        # ======== Column Not existing, then Add New Column
                                        vAssortedData = vAssortedData.withColumn(
                                                vColumn, split(col("sourceMedium"), " / ").getItem(0)
                                            )
                                        # ======== Is it successful?
                                        if self.is_ColumnExists(vAssortedData, vColumn) is True:
                                            if self._AddedColumns == 0:
                                                self.App["Response"]["Schema"]["Columns"]["Dimensions"]["Added"] = set()
                                            self._AddedColumns += 1
                                            self.App["Response"]["Schema"]["Columns"]["Dimensions"]["Added"].add(vColumn)
                                            self.App["Response"]["Schema"]["Columns"]["Metrics"]["Added"] = self._AddedColumns
                                            self.show_Info("Added Col", f"{vColumn} ::> Successful", "yellow")
                                        else:
                                            self.show_Info("Added Col", f"{vColumn} ::> Failed", "red")
                                    # ╔══════════════╗
                                    # ║  Add Column  ║
                                    # ╚══════════════╝
                                    vColumn = "Medium_Source-Name"
                                    if self.is_ColumnExists(vAssortedData, vColumn) is True:
                                        # ======== Column is already existing
                                        self.show_Info("Added Col", f"{vColumn} ::> Failed (Existing)", "red")
                                    else:
                                        # ======== Column Not existing, then Add New Column
                                        vAssortedData = vAssortedData.withColumn(
                                                vColumn, split(col("sourceMedium"), " / ").getItem(1)
                                            )
                                        # ======== Is it successful?
                                        if self.is_ColumnExists(vAssortedData, vColumn) is True:
                                            self._AddedColumns += 1
                                            self.App["Response"]["Schema"]["Columns"]["Dimensions"]["Added"].add(vColumn)
                                            self.App["Response"]["Schema"]["Columns"]["Metrics"]["Added"] = self._AddedColumns
                                            self.show_Info("Added Col", f"{vColumn} ::> Successful", "yellow")
                                        else:
                                            self.show_Info("Added Col", f"{vColumn} ::> Failed", "red")

                                case _:
                                    pass


                        case "UA":
                            pass

                        case _:
                            self.reset_Status("InvalidTargetMedia")
                    
                    # ╔════════════════════════════════════════╗
                    # ║ Transformation Area - Dropping Columns ║
                    # ╚════════════════════════════════════════╝
                    # ======== Scour each attribute
                    for vAttribute in self._DataAttributes:
                        # ======== Scour each column
                        for vColumn, vColumnSpecs in vAppConfigMediaClass["Schema"][vAttribute].items():
                            if "Drop" in vColumnSpecs and vColumnSpecs["Drop"] is True:
                                if self.is_ColumnExists(vAssortedData, vColumn) is True:
                                    # ======== Column is existing, then Delete the Column
                                    vAssortedData = vAssortedData.drop(vColumn)
                                    if self.is_ColumnExists(vAssortedData, vColumn) is True:
                                        self.show_Info("Deleted Col", f"{vColumn} ::> Failed", "red")
                                    else:
                                        if self._PurgedColumns == 0:
                                            self.App["Response"]["Schema"]["Columns"]["Dimensions"]["Purged"] = set()
                                        self._PurgedColumns += 1
                                        self.App["Response"]["Schema"]["Columns"]["Dimensions"]["Purged"].add(vColumn)
                                        self.App["Response"]["Schema"]["Columns"]["Metrics"]["Purged"] = self._PurgedColumns
                                        self.show_Info("Deleted Col", f"{vColumn} ::> Successful", "yellow")
                                else:
                                    self.show_Info("Deleted Col", f"{vColumn} ::> Failed (Not Existing)", "red")
                    # ╔═══════════════════════════════╗
                    # ║ Transformation Area - Sorting ║
                    # ╚═══════════════════════════════╝
                    is_Sorted = False
                    if "Sort" in vSchemaTransformation and vSchemaTransformation["Sort"]:
                        """
                        ╔════════════════════════════════════════════╗
                        ║ PROPER USAGE of SORTING by MULTIPLE FIELDS ║
                        ╚════════════════════════════════════════════╝
                        "Media" : {
                            "<MediaType>" : {
                                "Class" : {
                                    "<MediaClass>" : {
                                        "Schema" : {
                                            "Transformation" : {
                                                "Sort" : {
                                                    "Field1" : "ASC",
                                                    "Field2" : "DESC"
                                                }
                                            }
                        """
                        vSortColumns = []
                        for vSortField, vSortOrder in vSchemaTransformation["Sort"].items():
                            if vSortOrder.upper() == "ASC":
                                vSortColumns.append(vAssortedData[vSortField].asc())
                                self.show_Info("Sorted By", f"{vSortField} ::> Ascending", "yellow")
                                is_Sorted = True
                            elif vSortOrder.upper() == "DESC":
                                vSortColumns.append(vAssortedData[vSortField].desc())
                                self.show_Info("Sorted By", f"{vSortField} ::> Descending", "yellow")
                                is_Sorted = True
                    if is_Sorted is True:
                        # ======== Transfer the Sorted Data
                        vDataLoad = vAssortedData.orderBy(*vSortColumns)
                    else:
                        # ======== No Sorting happened
                        vDataLoad = vAssortedData
                    # ╔════════════════════════════════════════╗
                    # ║ Transformation Area - Renaming Columns ║
                    # ╚════════════════════════════════════════╝
                    # Renaming should be at the end of transformation stage
                    for vAttribute in self._DataAttributes:
                        # ======== Scour each column
                        for vColumn, vColumnSpecs in vAppConfigMediaClass["Schema"][vAttribute].items():
                            if "Rename" in vColumnSpecs and vColumnSpecs["Rename"] is not None:
                                vNewName = vColumnSpecs["Rename"]
                                if self.is_ColumnExists(vDataLoad, vColumn) is True:
                                    # ======== Column is existing, then Rename the Column
                                    vDataLoad = vDataLoad.withColumnRenamed(vColumn, vNewName)
                                    if self.is_ColumnExists(vDataLoad, vColumn) is True:
                                        self.show_Info("Renamed Col", f"{vColumn} to '{vNewName}' ::> Failed", "red")
                                    else:
                                        self._RenamedColumns += 1
                                        self.App["Response"]["Schema"]["Columns"]["Dimensions"]["Renamed"][vColumn] = vNewName
                                        self.App["Response"]["Schema"]["Columns"]["Metrics"]["Renamed"] = self._RenamedColumns
                                        self.show_Info("Renamed Col", f"{vColumn} to '{vNewName}' ::> Successful", "yellow")
                                else:
                                    self.show_Info("Renamed Col", f"{vColumn} ::> Failed (Not Existing)", "red")
                    # ╔═══════════════════════╗
                    # ║ End of Transformation ║
                    # ╚═══════════════════════╝
                    if self._ReturnStatus is not True:
                        raise MigrationError(
                            self.show_ErrorMsg()
                        )

                else:
                    # ======== No transformation
                    vDataLoad = vAssortedData
                vDataLoadCols = vDataLoad.columns
                self.App["Response"]["Schema"]["Columns"]["Dimensions"]["Total"] = vDataLoadCols
                self.App["Response"]["Schema"]["Columns"]["Metrics"]["Total"] = len(vDataLoadCols)
                # ╔══════════════════════════════════╗
                # ║ End of Successful Source Reading ║
                # ╚══════════════════════════════════╝
            if self._Target == "DZ" and self.AppConfig["Main"]["Switchboard"]["DebugMode"] is True:
                # ======== Skipped Transfer if this is a Debug Run
                self._Target = "Read"
            # ╔═══════════════╗
            # ║ Transfer Area ║
            # ╚═══════════════╝
            if self._Target == "Read":
                # ======== No target destination
                self.show_Info("Target", "Skipped")
            else:
                vCallerSource = self.AppConfig["Main"]["Data"]["Label"][self._Source]
                if self._Source == "DS":
                    # ======== Define inserter with MediaType for API Call
                    vInserter = f"{self._MediaType} {vCallerSource}"
                else:
                    # ======== Define inserter without MediaType
                    vInserter = vCallerSource
                if self.App["Request"]["ADF"]["PipelineCaller"] is not None:
                    vPipelineCaller = f'{vInserter} - {self.App["Request"]["ADF"]["PipelineCaller"]} Pipeline - {self.App["Request"]["ADF"]["PipelineName"]} ({self.App["Request"]["ADF"]["PipelineRunID"]}) - {self.App["Request"]["ADF"]["TriggerType"]} ({self.App["Request"]["ADF"]["TriggeredOn"]})'
                elif vRunner:
                    vPipelineCaller = f'{vInserter} - {self.App["Request"]["Caller"]} - {vRunner}'
                else:
                    vPipelineCaller = f'{vInserter} - {self.App["Request"]["Caller"]}'
                # ╔══════════════════╗
                # ║ Audit Row Trails ║
                # ╚══════════════════╝
                # This will overwrite the Pipeline Audit Trail if there is a Target and run directly.
                vAssortedData = vAssortedData.withColumn(
                        "Row_Inserted-On", 
                        date_format(
                                current_timestamp(), "yyyy-MM-dd HH:mm:ss"
                            ).cast(TimestampType())
                    ).withColumn(
                        "Row_Inserted-By", 
                        lit(vPipelineCaller)
                    ).withColumn(
                        "Row_Updated-On", 
                        date_format(
                                current_timestamp(), "yyyy-MM-dd HH:mm:ss"
                            ).cast(TimestampType())
                    ).withColumn(
                        "Row_Updated-By", 
                        lit(vPipelineCaller)
                     )
                # ======== Reorder columns and put Audit columns at the end
                vRegularColumns = [col for col in vAssortedData.columns if not col.startswith("Row_")]
                vAuditColumns = [col for col in vAssortedData.columns if col.startswith("Row_")]
                vOrderedColumns = vRegularColumns + vAuditColumns
                vAssortedData = vAssortedData.select(vOrderedColumns)
                is_ValidTarget = True
                vAppConfigStageTarget = self.AppConfig["Media"][self._MediaType]["Stage"][self._Target]
                match vAppConfigStageTarget["Provider"]["Type"]:
                    case "SQLServer":
                        # ======== Populate SQL Server credentials from Vault
                        vDBServer = dbutils.secrets.get(
                                scope=vAppConfigStageTarget["Secret"]["DB_Server"]["Scope"], 
                                key=vAppConfigStageTarget["Secret"]["DB_Server"]["Key"]
                            )
                        vDBName = dbutils.secrets.get(
                                scope=vAppConfigStageTarget["Secret"]["DB_Name"]["Scope"], 
                                key=vAppConfigStageTarget["Secret"]["DB_Name"]["Key"]
                            )
                        vDBUser = dbutils.secrets.get(
                                scope=vAppConfigStageTarget["Secret"]["DB_User"]["Scope"], 
                                key=vAppConfigStageTarget["Secret"]["DB_User"]["Key"]
                            ) 
                        vDBPass = dbutils.secrets.get(
                                scope=vAppConfigStageTarget["Secret"]["DB_Pass"]["Scope"], 
                                key=vAppConfigStageTarget["Secret"]["DB_Pass"]["Key"]
                            )
                        vDBTable = f'{vAppConfigStageTarget["Provider"]["Schema"]}.[{self._MediaType}-{vTargetName}]'
                        vTargetLoc = vDBTable
                        # ======== Write GA4 Data on the SQL Server
                        self.write_Database (
                                vDataLoad,
                                vDBServer,
                                vDBName,
                                vDBUser,
                                vDBPass,
                                vDBTable,
                                vAppConfigStageTarget["Provider"]
                            )

                    case "Azure/Blob":
                        # ======== Define target file path for blob
                        vTargetLoc = self.get_MediaPath(
                            vPath_ParentDir,
                            self._MediaType,
                            vMediaClassTarget,
                            self._Target,
                            vAppConfigStageTarget["Provider"]["SubDir"]["Path"],
                            vAppConfigStageTarget["Provider"]["SubDir"]["Extension"]
                        )
                        # ======== Write Blob
                        self.write_AzureBlob(
                                vDataLoad,
                                vAppConfigStageTarget["Secret"]["StorageAccount"]["Scope"],
                                vAppConfigStageTarget["Secret"]["StorageAccount"]["Key"],
                                vAppConfigStageTarget["Secret"]["StorageContainer"]["Scope"],
                                vAppConfigStageTarget["Secret"]["StorageContainer"]["Key"],
                                vTargetLoc,
                                vAppConfigStageTarget["Provider"]
                            )

                    case _:
                        is_ValidTarget = False
                        self.reset_Status("InvalidTargetType")

                # ======== Check if write operation was successful
                if is_ValidTarget is True:
                    if self._ReturnStatus is True:
                        self.show_Info("Target", f"{vTargetLoc} ::> Successful")
                    else:
                        self.show_Info("Target", f"{vTargetLoc} ::> Failed", "red")
                else:
                    raise MigrationError(
                        self.show_ErrorMsg()
                    )
            # ======== Write to Catalog ["LZ", "RZ", "BL", "SL", "GL", "DZ"]
            if self._Target != "Read" and self.AppConfig["Main"]["Switchboard"]["CatalogWrite"] is True: 
                # ======== Define catalog table path
                vCatalogTable = f"{vPath_ParentDir}.`{self._MediaType}-{vTargetName}-{self._Target}`"
                # ======== Create Catalog table
                self.write_Catalog(
                    vDataLoad,
                    vCatalogTable
                )
                if self._ReturnStatus is True:
                    # ======== Successful Catalog Write
                    self.show_Info("Catalog", f"{vCatalogTable} ::> Successful")
                else:
                    # ======== Failed Catalog Write but continue
                    self.show_Info("Catalog", f"{vCatalogTable} ::> Failed", "red")
            # ======== Check if display table switch is enabled
            vDisplayPath = f"{vPath_ParentDir}/{self._MediaType}/{self._MediaClass}"
            if self.AppConfig["Main"]["Switchboard"]["DisplayTable"] is True:
                # ======== Display the assorted data, which will be sorted by the Module:Transform_Data in Silver Layer
                display(vDataLoad)
                # ======== Successfuly display
                self.show_Info("Displayed", f"{vDisplayPath} ::> Successful")
            else:
                # ======== Skipped display
                self.show_Info("Displayed", f"{vDisplayPath} ::> Skipped")
            vDataLoad.unpersist()
        # ======== Handle specific for this module
        except MigrationError as MigrationErrorMsg:
            is_MigrationError = True
            TraceMsg = MigrationErrorMsg
        # ======== Handle other exceptions
        except Exception as ExceptionError:
            TraceMsg = ExceptionError
        else:
            # ======== Mark as Successful
            vStartSuccess = True
        finally:
            is_ActivityStat = self.AppConfig["Main"]["Switchboard"]["ActivityStat"]
            if self.AppConfig["Main"]["Switchboard"]["TestMode"] is True:
                # ======== Turn On visual cue, Overriding previous setting
                is_ActivityStat = False
            # ======== Compile the Responses
            self.App["Response"]["Success"] = vStartSuccess
            # ======== Clear Failure flags
            self.App["Response"]["Failure"]["Code"] = None
            self.App["Response"]["Failure"]["Filename"] = None
            self.App["Response"]["Failure"]["Function"] = None
            self.App["Response"]["Failure"]["Line"] = 0
            self.App["Response"]["Failure"]["Simulated"] = is_ErrorSimulated
            vEndDT = datetime.now()
            self.App["Response"]["Header"]["App"]["Statistics"]["Ended"] = self.get_PHdatetime(vEndDT)
            self.App["Response"]["Header"]["App"]["Statistics"]["Duration"] = str(vEndDT - vStartDT)
            if vStartSuccess is False:
                self.App["Response"]["Status"] = 404
                self.App["Response"]["Failure"]["Message"]["Display"] = None
                self.App["Response"]["Failure"]["Message"]["Exception"] = self.clean_Exception()
                if TraceMsg is not None:
                    # ======== Set the error message that will be fetched by the ADF Fail Activity
                    vDisplayTrace = str(TraceMsg)
                    self.App["Response"]["Failure"]["Message"]["Display"] = vDisplayTrace.lstrip()
                    err_File, err_Line, err_Func, err_Code = None, None, None, None
                    if is_MigrationError is True:
                        self.show_Text(f"{TraceMsg}\r\n", "red")
                        # ======== Remove duplicate error message
                        TraceMsg = None
                    else:
                        # ======== Extract traceback information
                        exp_Trace = traceback.extract_tb(TraceMsg.__traceback__)
                        # ======== Check if traceback is available
                        if exp_Trace:
                            # ======== Get the top frame of the traceback
                            err_TopFrame = exp_Trace[0]
                            # ======== Extract error details
                            err_File, err_Line, err_Func, err_Code = err_TopFrame
                    self.App["Response"]["Failure"]["Code"] = err_Code
                    self.App["Response"]["Failure"]["Filename"] = err_File
                    self.App["Response"]["Failure"]["Function"] = err_Func
                    self.App["Response"]["Failure"]["Line"] = err_Line
                    # ======== Display Exception and log error
                    self.show_Exception(
                        TraceMsg,
                        dbutils,
                        self.App["Metadata"]["Name"],
                        self.App["Metadata"]["Version"],
                        self.App["Metadata"]["Datahouse"],
                        vCompleteCaller,
                        self.App["Response"],
                        err_Line, 
                        err_Func, 
                        err_Code
                    )
                # ======== Failed Operation
                if self._Target == "Read":
                    self.show_Info("Reading", "Failed", "red")
                else:
                    self.show_Info("Transfer", "Failed", "red")
                # ======== Is Pipeline Activity Status enabled?
                if is_ActivityStat is True:
                    dbutils.notebook.exit("Failure")
            else:
                self.App["Response"]["Status"] = 200
                self.log_Success (
                    self.App["Response"],
                    vCompleteCaller,
                    self.App["Metadata"]["Name"],
                    self.App["Metadata"]["Version"],
                    self.App["Metadata"]["Datahouse"],
                    dbutils
                )
                # ======== Successful Operation
                if self._Target == "Read":
                    self.show_Info("Reading", "Successful", "green")
                else:
                    self.show_Info("Transfer", "Successful", "green")
                # ======== Is Pipeline Activity Status enabled?
                if is_ActivityStat is True:
                    dbutils.notebook.exit("Success")
            dbutils.library.restartPython()
            # ======== Check if this is a Testing Run
            if self.AppConfig["Main"]["Switchboard"]["TestMode"] is True:
                return vStartSuccess
            else:
                return None

    def Read_TotalRows (
            self,
            arg_MediaType,
            arg_MediaClasses,
            arg_DataLayers
        ):
        is_Success = False
        from datetime import datetime
        vStartDT = datetime.now()
        self.show_Info("Started", f"{self.get_PHdatetime(vStartDT)}")
        for vMediaClass in arg_MediaClasses:
            for vDataLayer in arg_DataLayers:
                is_Success = False
                self.LibConfig["Main"]["Switchboard"]["SilentMode"] = True
                self.Start(arg_MediaType, vMediaClass, vDataLayer, "Read")
                self.LibConfig["Main"]["Switchboard"]["SilentMode"] = False
                if self.App["Response"]["Success"]:
                    is_Success = True
                    if self._TotalRows:
                        self.show_Info("Total Rows", f"{arg_MediaType}\\{vMediaClass}\\{vDataLayer} ::> {format(self._TotalRows, ',')}")
                else:
                    self.show_Info("Error", self.App["Response"]["Failure"]["Message"]["Display"], "red")
                    vException = self.App["Response"]["Failure"]["Message"]["Exception"]
                    if vException is not None or not isinstance(vException, bool):
                        self.show_Info("Exception", str(vException), "red")
                    break
            print('')
        if is_Success is True:
            self.show_Info("Reading", "Successful", "green")
        else:
            self.show_Info("Reading", "Failed", "red")
        vEndDT = datetime.now()
        self.show_Info("Ended", f"{self.get_PHdatetime(vEndDT)}")
        self.show_Info("Duration", f"{vEndDT - vStartDT}")
        print('')
        # ======== Check if this is a Testing Run
        if self.AppConfig["Main"]["Switchboard"]["TestMode"] is True:
            return is_Success
        else:
            return None

    def Read_Checksum (
            self,
            arg_MediaType,
            arg_MediaClasses,
            arg_DataLayers
        ):
        is_Success = False
        from datetime import datetime
        vStartDT = datetime.now()
        self.show_Info("Started", f"{self.get_PHdatetime(vStartDT)}")
        vPrevDL = None
        vPrevMC = None
        vChecksum = None
        for vCurrMC in arg_MediaClasses:
            for vCurrDL in arg_DataLayers:
                is_Success = False
                self.LibConfig["Main"]["Switchboard"]["SilentMode"] = True
                self.AppConfig["Main"]["Switchboard"]["IntegrityCheck"] = True
                self.Start(arg_MediaType, vCurrMC, vCurrDL, "Read")
                self.LibConfig["Main"]["Switchboard"]["SilentMode"] = False
                if self.App["Response"]["Success"]:
                    is_Success = True
                    if self.App["Response"]["Schema"]["Integrity"]["Checksum"]:
                        if vChecksum is not None and vPrevDL is not None and vPrevMC == vCurrMC:
                            if vChecksum != self.App["Response"]["Schema"]["Integrity"]["Checksum"]:
                                self.show_Info("Findings", f"{arg_MediaType}\\{vCurrMC} ::> [{vCurrDL}] is not equal to [{vPrevDL}]", "red")
                            else:
                                self.show_Info("Findings", f"{arg_MediaType}\\{vCurrMC} ::> [{vCurrDL}] is the same with [{vPrevDL}]", "green")
                        else:
                            self.show_Info("Findings", f"{arg_MediaType}\\{vCurrMC}\\{vCurrDL} ::> [Baseline]")
                        self.show_Info("Checksum", self.App["Response"]["Schema"]["Integrity"]["Checksum"])
                        print('')
                        vPrevDL = vCurrDL
                        vPrevMC = vCurrMC
                        vChecksum = self.App["Response"]["Schema"]["Integrity"]["Checksum"]
                else:
                    self.show_Info("Error", self.App["Response"]["Failure"]["Message"]["Display"], "red")
                    vException = self.App["Response"]["Failure"]["Message"]["Exception"]
                    if vException is not None or not isinstance(vException, bool):
                        self.show_Info("Exception", str(vException), "red")
                    print('')
                    break
        self.LibConfig["Main"]["Switchboard"]["SilentMode"] = False
        if is_Success is True:
            self.show_Info("Checksum", "Successful", "green")
        else:
            self.show_Info("Checksum", "Failed", "red")
        vEndDT = datetime.now()
        self.show_Info("Ended", f"{self.get_PHdatetime(vEndDT)}")
        self.show_Info("Duration", f"{vEndDT - vStartDT}")
        print('')
        # ======== Check if this is a Testing Run
        if self.AppConfig["Main"]["Switchboard"]["TestMode"] is True:
            return is_Success
        else:
            return None

    def Display_Layer (
            self,
            arg_MediaType,
            arg_MediaClasses,
            arg_SourceLayers,
            arg_isDebug
        ):
        is_Success = False
        from datetime import datetime
        vStartDT = datetime.now()
        self.show_Info("Started", f"{self.get_PHdatetime(vStartDT)}")
        for vMediaClass in arg_MediaClasses:
            for vSourceLayer in arg_SourceLayers:
                is_Success = False
                self.AppConfig["Main"]["Switchboard"]["DebugMode"] = arg_isDebug
                self.AppConfig["Main"]["Switchboard"]["DisplayTable"] = True
                self.Start(arg_MediaType, vMediaClass, vSourceLayer, "Read")
                print('')
                if self.App["Response"]["Success"]:
                    is_Success = True
                else:
                    break
        if is_Success is True:
            self.show_Info("Display", "Successful", "green")
        else:
            self.show_Info("Display", "Failed", "red")
        vEndDT = datetime.now()
        self.show_Info("Ended", f"{self.get_PHdatetime(vEndDT)}")
        self.show_Info("Duration", f"{vEndDT - vStartDT}")
        print('')
        # ======== Check if this is a Testing Run
        if self.AppConfig["Main"]["Switchboard"]["TestMode"] is True:
            return is_Success
        else:
            return None

    def Run_Transfer (
            self,
            arg_MediaType,
            arg_MediaClasses,
            arg_Lineage
        ):
        is_Success = False
        from datetime import datetime
        vStartDT = datetime.now()
        self.show_Info("Started", f"{self.get_PHdatetime(vStartDT)}")
        for vMediaClass in arg_MediaClasses:
            for vSource, vTarget in arg_Lineage.items():
                is_Success = False
                self.AppConfig["Main"]["Switchboard"]["DebugMode"] = True
                self.Start(arg_MediaType, vMediaClass, vSource, vTarget)
                print('')
                if self.App["Response"]["Success"]:
                    is_Success = True
                else:
                    break
        if is_Success is True:
            self.show_Info("Transfer", "Successful", "green")
        else:
            self.show_Info("Transfer", "Failed", "red")
        vEndDT = datetime.now()
        self.show_Info("Ended", f"{self.get_PHdatetime(vEndDT)}")
        self.show_Info("Duration", f"{vEndDT - vStartDT}")
        print('')
        # ======== Check if this is a Testing Run
        if self.AppConfig["Main"]["Switchboard"]["TestMode"] is True:
            return is_Success
        else:
            return None

class Data_Specs:
    App = {
        "Metadata" : {
            "Name" : "Data_Migration",
            "Version" : 1.05,
            "Datahouse" : "Org Datamart",
            "Repository" : "org-datamart"
        },
        "Request" : {
            # ======== Fetch App Configuration from CSV File
            "ConfigFile" : False,
            "Caller" : "Direct", # If will be run by ADF, indicate "Pipeline"
            # Data Integration Service (ADF) Specifications
            "ADF" :{
                "Name" : None,
                "PipelineName" : None,
                "PipelineRunID" : None,
                "PipelineCaller" : None,
                "TriggerID" : None,
                "TriggerName" : None,
                "TriggerType" : None,
                "TriggeredOn" : None,
                "Payload": {}
            }
        },
        "Response" : {
            "Status" : 200,
            "Header" : {
                "App" : {
                    "Caller" : {},
                    "Statistics" : {}
                }
            },
            "Failure" : {
                "Message" : {
                    "Exception" : None,
                    "Display" : None
                },
                "Line" : 0,
                "Function" : None,
                "Code" : None,
                "Filename" : None
            },
            "Schema" : {
                "Columns" : {
                    "Dimensions" : {
                        "Added" : {},
                        "Purged" : {},
                        "Updated" : {},
                        "Renamed" : {},
                        "Total" : {}
                    },
                    "Metrics" : {
                        "Added" : 0,
                        "Purged" : 0,
                        "Updated" : 0,
                        "Renamed" : 0,
                        "Total" : 0
                    }
                },
                "Rows" : {
                    "Duplicates" : 0,
                    "GrossTotal" : 0,
                    "NetTotal" : 0
                },
                "Quality" : {
                    "Null" : {},
                    "Blank" : {},
                    "Unique" : {},
                    "UserDefined" : {}
                },
                "Integrity" : {
                    "Checksum" : None
                }
            }
        }
    }
    AppConfig = {
        "Main" : {
            "Switchboard" : {
                # ======== Display Table after successful transfer
                "DisplayTable" : False,
                "ExceptionApp" : False,
                "ExceptionAll" : False,
                "CatalogWrite" : True,
                # ======== Enable Pipeline Activity Status?
                # Automatically will be set to False if ["Main"]["Switchboard"]["TestMode"] is True
                "ActivityStat" : False,
                # ======== Verify the Quality of Data
                "QualityCheck" : True,
                # ======== Verify the Integrity of data
                "IntegrityCheck" : False,
                # ======== Suppress showing of display message
                "SilentMode" : False,
                "TestMode" : False,
                "DebugMode" : False
            },
            "Data" : {
                # ======== Source Layer            
                "Source" : {"DS", "LZ", "RZ", "BL", "SL", "GL"},
                # ======== Target can be "Auto", which automatically assigned the target layer
                "Target" : {"LZ", "RZ", "BL", "SL", "GL", "DZ", "Read"},
                # ======== Source Layer that needs to be transformed
                "Transformer" : "BL", # put None if there's no transformation layer
                # ======== Source Layer that needs to be aggregated, filtered or combined with other data sources
                "Merger" : "SL",
                # ======== Source and their Target layer
                "Lineage" : {
                    "DS" : "LZ",
                    "LZ" : "RZ",
                    "RZ" : "BL",
                    "BL" : "SL",
                    "SL" : "GL",
                    "GL" : "DZ"
                },
                # ======== Label for each layer
                "Label" : {
                    "DS" : "API",
                    "LZ" : "EDLZ",
                    "RZ" : "Raw Zone",
                    "BL" : "Bronze Layer",
                    "SL" : "Silver Layer",
                    "GL" : "Gold Layer",
                    "DZ" : "SQL Server",
                    "Read" : "Read Only",
                    "Auto" : "Automatic"
                },
                # ======== User-defined expressions used in Data Quality
                "Expressions" :  {
                    "alphanumeric" : r'^[a-zA-Z0-9]+$'
                },
                "Audit" : {
                    "Database" : {
                        "Enabled" : True,
                        "Table" : "Pipeline"
                    },
                    "Catalog" : {
                        "Enabled" : True,
                        "Schema" : "Pipeline"
                    }
                }
            }
        },
        "Media" : {
            "GA4" : {
                "Common" : {
                    "Directory" : {
                        "Live" : "Google",
                        "Debug" : "Test"
                    }
                },
                "Class" : {
                    # ╔═══════════╗
                    # ║ Demo GA4  ║
                    # ╚═══════════╝
                    "Demo_GA4" : {
                        "Directory" : {
                            "Live" : "Google",
                            "Debug" : "Test"
                        },
                        "API" : {
                            "PropertyID" : "12345678",
                            "StartDate" : "2025-01-01",
                            "EndDate" : None,
                            "Fetch_Offset" : 0,
                            "Fetch_Limit" : 100000
                        },
                        "Schema" : {
                            "Quality" : {
                                "Duplicates" : {
                                    "Remove" : False,
                                    "Fields" : {"*"}
                                }
                            },
                            "Transformation" : {
                                "Filter" : {
                                    "OR" : {
                                        "==" : {
                                            # Only include Canada and New Zealand
                                            "countryId" : {
                                                "CA",
                                                "NZ"
                                            }
                                        }
                                    }
                                },
                                "New" : {
                                    "Divide" : {
                                        "NewFieldName" : {
                                            "ExistingField1" : "ExistingField2"
                                        }
                                    },
                                    "Split" : {
                                        "NewFieldName" : {
                                            " / " : {
                                                "ExistingField1" : 1
                                            }
                                        }
                                    }
                                },
                                "Update" : {
                                    "Replace" : {
                                        "countryId" : {
                                            "_" : "XX"
                                        }
                                    },
                                    "ConcatSeparator" : {
                                        "ExistingField1" : {
                                            "-" : {
                                                1: 4,
                                                5: 2,
                                                7: 2
                                            }
                                        }
                                    }
                                },
                                "Group" : {
                                    "By" : {
                                        "ExistingField1", 
                                        "ExistingField2"
                                    },
                                    "Agg" : {
                                        "ExistingField3:alias" : "SUM",
                                        "ExistingField4:alias" : "AVG"
                                    }
                                },
                                "Sort" : {
                                    "date" : "ASC"
                                }
                            },
                            "Dimensions" : {
                                "date" : {
                                    "Type" : "date",
                                    "Drop" : False,
                                    "Rename" : "Date"
                                },
                                "countryId" : {
                                    "Type" : "string",
                                    "Drop" : False,
                                    "Rename" : "RK-MediumCountry",
                                    "Quality" : {
                                        "Null", "Blank", "Unique", "UserDefined:alphanumeric"
                                    }
                                },
                                "sourceMedium" : {
                                    "Type" : "string",
                                    "Drop" : False,
                                    "Rename" : "Source_Medium",
                                    "Quality" : {
                                        "Null", "Blank", "Unique", "UserDefined:alphanumeric"
                                    }
                                },
                            },
                            "Metrics" : {
                                "sessions" : {
                                    "Type" : "integer",
                                    "Drop" : False,
                                    "Rename" : "Sessions"
                                }
                            }
                        }
                    }
                },
                "Stage" : {
                    "DS" : {
                        "Provider" : {
                            "Type" : "API/GA4",
                            "Account" : {
                                "Type" : "service_account",
                                "URL_Auth" : "https://accounts.google.com/o/oauth2/auth",
                                "URL_Token" : "https://oauth2.googleapis.com/token"
                            }
                        },
                        "Secret" : {
                            "ProjectID" : {
                                "Scope" : "DemoVault",
                                "Key" : "org-ga4-service-account-project-id"
                            },
                            "PrivateKeyID" : {
                                "Scope" : "DemoVault",
                                "Key" : "org-ga4-service-account-private-key-id"
                            },
                            "PrivateKey" : {
                                "Scope" : "DemoVault",
                                "Key" : "org-ga4-service-account-private-key"
                            },
                            "ClientEmail" : {
                                "Scope" : "DemoVault",
                                "Key" : "org-ga4-service-account-client-email"
                            },
                            "ClientID" : {
                                "Scope" : "DemoVault",
                                "Key" : "org-ga4-service-account-client-id"
                            },
                            "URLx509" : {
                                "Scope" : "DemoVault",
                                "Key" : "org-ga4-service-account-client-x509-cert-url"
                            }
                        }
                    },
                    "LZ" : { # EDLZ
                        "Provider" : {
                            "Type" : "Azure/Blob",
                            "Format" : "parquet",
                            "Write" : {
                                "Mode" : "overwrite",
                                "MaxRecordsPerFile" : 1000000,
                                "OverwriteSchema" : "true"
                            },
                            "SubDir" : {
                                "Path" : None,
                                "Extension" : None
                            }
                        },
                        "Secret" : {
                            "StorageAccount" : {
                                "Scope" : "DemoVault",
                                "Key" : "dl-name"
                            },
                            "StorageContainer" : {
                                "Scope" : "DemoVault",
                                "Key" : "dl-container"
                            }
                        }
                    },
                    "RZ" : { # Datamart - Raw Zone
                        "Provider" : {
                            "Type" : "Azure/Blob",
                            "Format" : "parquet",
                            "Write" : {
                                "Mode" : "overwrite",
                                "MaxRecordsPerFile" : 1000000,
                                "OverwriteSchema" : "true"
                            },
                            "SubDir" : {
                                "Path" : "y/m/d",   # YYYY/MM/DD directory
                                "Extension" : None
                            }
                        },
                        "Secret" : {
                            "StorageAccount" : {
                                "Scope" : "DemoVault",
                                "Key" : "dl-name"
                            },
                            "StorageContainer" : {
                                "Scope" : "DemoVault",
                                "Key" : "dl-container"
                            }
                        }
                    },
                    "BL" : { # Datamart - Bronze Layer
                        "Provider" : {
                            "Type" : "Azure/Blob",
                            "Format" : "delta",
                            "Write" : {
                                "Mode" : "overwrite",
                                "MaxRecordsPerFile" : 1000000,
                                "OverwriteSchema" : "true"
                            },
                            "SubDir" : {
                                "Path" : None,
                                "Extension" : None
                            }
                        },
                        "Secret" : {
                            "StorageAccount" : {
                                "Scope" : "DemoVault",
                                "Key" : "dl-name"
                            },
                            "StorageContainer" : {
                                "Scope" : "DemoVault",
                                "Key" : "dl-container"
                            }
                        }
                    },
                    "SL" : { # Datamart - Silver Layer
                        "Provider" : {
                            "Type" : "Azure/Blob",
                            "Format" : "delta",
                            "Write" : {
                                "Mode" : "overwrite",
                                "MaxRecordsPerFile" : 1000000,
                                "OverwriteSchema" : "true"
                            },
                            "SubDir" : {
                                "Path" : None,
                                "Extension" : None
                            }
                        },
                        "Secret" : {
                            "StorageAccount" : {
                                "Scope" : "DemoVault",
                                "Key" : "dl-name"
                            },
                            "StorageContainer" : {
                                "Scope" : "DemoVault",
                                "Key" : "dl-container"
                            }
                        }
                    },
                    "GL" : { # Datamart - Gold Layer
                        "Provider" : {
                            "Type" : "Azure/Blob",
                            "Format" : "delta",
                            "Write" : {
                                "Mode" : "overwrite",
                                "MaxRecordsPerFile" : 1000000,
                                "OverwriteSchema" : "true"
                            },
                            "SubDir" : {
                                "Path" : None,
                                "Extension" : None
                            }
                        },
                        "Secret" : {
                            "StorageAccount" : {
                                "Scope" : "DemoVault",
                                "Key" : "dl-name"
                            },
                            "StorageContainer" : {
                                "Scope" : "DemoVault",
                                "Key" : "dl-container"
                            }
                        }
                    },
                    "DZ" : { # Datamart - Destination Zone
                        "Provider" : {
                            "Type" : "SQLServer",
                            "Format" : "jdbc",
                            "Batch" : 10000,
                            "Write" : {
                                "Mode" : "overwrite"
                            },
                            "Port" : 1433,
                            "Schema" : "fact",
                            "Driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                            "Retry" : {
                                "Maximum" : 10, # Maximum number of retry attempts
                                "Delay" : 5 # Delay between retries in seconds
                            },
                            "Partition" : 40
                        },
                        "Secret" : {
                            "DB_Server" : {
                                "Scope" : "DemoVault",
                                "Key" : "sqldb-projorg-server"
                            },
                            "DB_Name" : {
                                "Scope" : "DemoVault",
                                "Key" : "sqldb-projorg-database"
                            },
                            "DB_User" : {
                                "Scope" : "DemoVault",
                                "Key" : "sqldb-projorg-user"
                            },
                            "DB_Pass" : {
                                "Scope" : "DemoVault",
                                "Key" : "proj-sql-server"
                            }
                        }
                    }
                } # ======== End of Stage
            }
        }
    }

"""
Module Name: Shared_MainLib.py

Description:
This module provides common reusable functions for transferring and transforming data

Debugging:v
self.Debug("<Debug Value>")

Author: Erwin Bernard Talento
Email: etalento.contractor@xxx.xxx
"""

class Shared_MainLib:
    AppLib = {
        "Metadata" : {
            "Name" : "Shared_MainLib",
            "Version" : 1.05,
            "Datahouse" : "Org Datamart",
            "Repository" : "org-datamart",
            "Environment" : {
                "Scope" : "DemoVault",
                "Key" : "datadog-env"
            }
        },
        "Request" : {
            # ======== Fetch Library Configuration from CSV File
            "ConfigFile" : False
        }
    }
    LibConfig = {
        "Main" : {
            "Switchboard" : {
                "FileLog" : False,
                "CloudLog" : True,
                "ExceptionApp" : False,
                "ExceptionAll" : False,
                # ======== Suppress showing of display message
                "SilentMode" : False,
                "TestMode" : False,
                "DebugMode" : False
            },            
            "Provider" : {
                "Datadog" : {
                    "SiteName" : "datadoghq.com",
                    "KeyAPI" : {
                        "Scope" : "DemoVault",
                        "Key"   : "datadog-api-key"
                    },
                    "Environment" : {
                        "Scope" : "DemoVault",
                        "Key"   : "datadog-env"
                    },
                    "ServiceName" : {
                        "Scope" : "DemoVault",
                        "Key"   : "datadog-service-name"
                    }
                }
            },
            "Error" : {
                "Message" : {
                    "SyntaxMediaType" : {
                        "Head" : "      Error ::> [Syntax] Invalid Media Type",
                        "Body" : "                Please use one of the following"
                    },
                    "SyntaxMediaClass" : {
                        "Head" : "      Error ::> [Syntax] Invalid Media Class",
                        "Body" : "                Please use one of the following"
                    },
                    "SyntaxSource" : {
                        "Head" : "      Error ::> [Syntax] Invalid Source",
                        "Body" : "                Please use one of the following"
                    },
                    "SyntaxTarget" : {
                        "Head" : "      Error ::> [Syntax] Invalid Target",
                        "Body" : "                Please use one of the following"
                    },
                    "SyntaxLineage" : {
                        "Head" : "      Error ::> [Syntax] Source and Target should not be the same"
                    },
                    "FailedGA4RowCount" : {
                        "Head" : "      Error ::> [API] Failed fetching GA4 Row Count"
                    },
                    "FailedGA4Report" : {
                        "Head" : "      Error ::> [API] Failed running GA4 report"
                    },
                    "FailedGA4Pull" : {
                        "Head" : "      Error ::> [API] Failed pulling GA4 data"
                    },
                    "FailedAppInit" : {
                        "Head" : "      Error ::> [App] Failed initializing the application"
                    },
                    "FailedLibInit" : {
                        "Head" : "      Error ::> [App] Failed initializing the library"
                    },
                    "FailedDFS" : {
                        "Head" : "      Error ::> [App] Failed fetching the DFS Path"
                    },
                    "FailedSourceRead" : {
                        "Head" : "      Error ::> [App] Failed reading the Source Data"
                    },
                    "FailedBlobRead" : {
                        "Head" : "      Error ::> [App] Failed reading the Blob Storage"
                    },
                    "FailedBlobWrite" : {
                        "Head" : "      Error ::> [App] Failed writing the Blob Storage"
                    },
                    "FailedDBWrite" : {
                        "Head" : "      Error ::> [App] Failed writing the Database"
                    },
                    "FailedCatalogWrite" : {
                        "Head" : "      Error ::> [App] Failed writing the Catalog"
                    },
                    "FailedDecryption" : {
                        "Head" : "      Error ::> [App] Failed decrypting data"
                    },
                    "FailedTypeSet" : {
                        "Head" : "      Error ::> [App] setting the data type"
                    },
                    "InvalidSourceMedia" : {
                        "Head" : "      Error ::> [App] Invalid Source Media"
                    },
                    "InvalidSourceType" : {
                        "Head" : "      Error ::> [App] Invalid Source Type"
                    },
                    "InvalidTargetMedia" : {
                        "Head" : "      Error ::> [App] Invalid Target Media"
                    },
                    "InvalidTargetType" : {
                        "Head" : "      Error ::> [App] Invalid Target Type"
                    },
                    "InvalidTargetClass" : {
                        "Head" : "      Error ::> [App] Invalid Target Class"
                    },
                    "FailedDDLogger" : {
                        "Head" : "      Error ::> [Provider] Datadog failed logging error"
                    },
                    "TestError" : {
                        "Head" : "      Error ::> [Simulated] This is only a test. Please ignore"
                    }
                }
            }
        }
    }

    def __init__ (
            self
        ):
        import traceback
        self.InitSuccess = False
        dbutils.library.restartPython()
        try:
            # ======== Initialize Shared Library
            self._TotalRows = 0
            self._TotalColumns = 0
            self._ColumnNames = []
            self._AddedColumns = 0
            self._PurgedColumns = 0
            self._UpdatedColumns = 0
            self._RenamedColumns = 0
            self._ErrorKey = False
            self._ErrorBody = False
            self._ErrorTail = False
            self._Exception = None
            self._ReturnValue = False
            self._ReturnStatus = False
            self.DFS_Path = ''
            # ======== Set error message that will be shown
            self.reset_Status("FailedLibInit")
            from datetime import datetime
            # ======== Record the starting date & time
            self._Started = datetime.now()
            if self.LibConfig["Main"]["Switchboard"]["CloudLog"] is True:
                try:
                    %pip show datadog_api_client > /dev/null 2>&1
                except Exception as ExceptionError:
                    self._Exception = ExceptionError
                    %pip install --upgrade datadog_api_client > /dev/null 2>&1
            # ======== Get the current environment
            self._Environment = dbutils.secrets.get(
                    self.AppLib["Metadata"]["Environment"]["Scope"],
                    self.AppLib["Metadata"]["Environment"]["Key"]
                )
            self._Environment = self._Environment.upper()
        except Exception as ExceptionError:
            exp_Trace = traceback.extract_tb(ExceptionError.__traceback__)
            if exp_Trace:
                err_TopFrame = exp_Trace[0]
                err_File, err_Line, err_Func, err_Code = err_TopFrame
                # ======== Show the error
                self.show_Exception(
                    ExceptionError,
                    dbutils,
                    self.AppLib["Metadata"]["Name"],
                    self.AppLib["Metadata"]["Version"],
                    self.AppLib["Metadata"]["Datahouse"],
                    'Module',
                    None,
                    err_Line, 
                    err_Func, 
                    err_Code
                )
            else:
                # ======== Show the error without a stack trace
                self.show_Exception(
                    ExceptionError,
                    dbutils,
                    self.AppLib["Metadata"]["Name"],
                    self.AppLib["Metadata"]["Version"],
                    self.AppLib["Metadata"]["Datahouse"],
                    'Module'
                )
            # ======== Put at the end to eliminate the overwriting of the original exception
            self._Exception = ExceptionError
        else:
            self.InitSuccess = True
        finally:
            self.show_Text(f'\r\n    Library ::> {self.AppLib["Metadata"]["Name"]} v{self.AppLib["Metadata"]["Version"]}', "magenta")
            if self.InitSuccess is True:
                self.show_Info("Init", "Successful\r\n", "green")
            else:
                self.show_Info("Init", "Failed\r\n", "red")
        return

    def reset_Status (
            self,
            arg_ErrorKey = False,
            arg_ErrorTail = False,
            arg_ErrorBody = False,
            arg_Exception = None,
            arg_ReturnStatus = False,
            arg_ReturnValue = False,
        ):
        try:
            # ======== Reset the Error Status and set the error message
            self._ErrorKey = arg_ErrorKey
            self._ErrorTail = arg_ErrorTail
            self._ErrorBody = arg_ErrorBody
            self._Exception = arg_Exception
            self._ReturnStatus = arg_ReturnStatus
            self._ReturnValue = arg_ReturnValue
            return True
        except Exception as ExceptionError:
            self._Exception = ExceptionError
            return False
        
    def get_AzureDFS (
            self, 
            arg_AccountScope,
            arg_AccountKey,
            arg_ContainerScope,
            arg_ContainerKey,
            arg_StoragePath
        ):
        try:
            # ======== Set error message that will be shown
            self.reset_Status("FailedDFS", arg_StoragePath)
            # ======== Get the Storage Account
            vStorageAccount = dbutils.secrets.get(
                scope=arg_AccountScope, 
                key=arg_AccountKey
            )
            if vStorageAccount:
                # ======== Get the Container Name
                vStorageContainer = dbutils.secrets.get(
                    scope=arg_ContainerScope, 
                    key=arg_ContainerKey
                )
                if vStorageContainer:
                    # ======== Assemble the domain path, directory and file
                    vPath_Domain = f"abfss://{vStorageContainer}@{vStorageAccount}.dfs.core.windows.net"
                    # ======== Google/GA4/BL/GA4-Blog_Daily-BL
                    self._ReturnValue = f"{vPath_Domain}/{arg_StoragePath}"
                    self._ReturnStatus = True
        except Exception as ExceptionError:
            self._Exception = ExceptionError
        return self._ReturnStatus
    
    def read_AzureBlob (
            self, 
            arg_AccountScope,
            arg_AccountKey,
            arg_ContainerScope,
            arg_ContainerKey,
            arg_StoragePath,
            arg_Config
        ):
        try:
            # ======== Get the Distributed File System Path
            self.get_AzureDFS(
                    arg_AccountScope,
                    arg_AccountKey,
                    arg_ContainerScope,
                    arg_ContainerKey,
                    arg_StoragePath
                )
            if self._ReturnStatus is True:
                retVal1 = self._ReturnValue
                # ======== Set error message that will be shown
                self.reset_Status("FailedBlobRead", self._ErrorTail) # get the Path from the previous call
                retVal2 = spark.read.format(arg_Config["Format"]).load(retVal1)
                if retVal2:
                    if self.LibConfig["Main"]["Switchboard"]["TestMode"] is False:
                        # Only fetch when there's no testing as Unit Testing is failing here
                        from pyspark import StorageLevel
                        retVal2.persist(StorageLevel.MEMORY_AND_DISK)
                        self._TotalRows = retVal2.count()
                        vTotalRows = format(self._TotalRows, ',')
                        self.show_Info("Fetched", f"Blob Total Count ({vTotalRows} rows) ::> Successful")
                    # ======== Save Total Columns and Column Names
                    self._TotalColumns = len(retVal2.columns)
                    self._ColumnNames = [field.name for field in retVal2.schema.fields]
                    self._ReturnValue = retVal2
                    self._ReturnStatus = True
        except Exception as ExceptionError:
            self._Exception = ExceptionError
        return self._ReturnStatus

    def write_AzureBlob (
            self, 
            arg_DataFrame,
            arg_AccountScope,
            arg_AccountKey,
            arg_ContainerScope,
            arg_ContainerKey,
            arg_StoragePath,
            arg_Config
        ):
        try:
            # ======== Get the Distributed File System Path
            self.get_AzureDFS(
                    arg_AccountScope,
                    arg_AccountKey,
                    arg_ContainerScope,
                    arg_ContainerKey,
                    arg_StoragePath
                )
            if self._ReturnStatus is True:
                retVal1 = self._ReturnValue
                # ======== Set error message that will be shown
                self.reset_Status("FailedBlobWrite", self._ErrorTail) # get the Path from the previous call
                self._ReturnValue = arg_DataFrame.write.format(arg_Config["Format"]) \
                    .mode(arg_Config["Write"]["Mode"]) \
                    .option("maxRecordsPerFile", arg_Config["Write"]["MaxRecordsPerFile"]) \
                    .option("overwriteSchema", arg_Config["Write"]["OverwriteSchema"]) \
                    .save(retVal1)
                self._ReturnStatus = True
        except Exception as ExceptionError:
            self._Exception = ExceptionError
        return self._ReturnStatus

    def write_Catalog (
            self,
            arg_DataFrame,
            arg_Table
        ):
        try:
            # ======== Set error message that will be shown
            self.reset_Status("FailedCatalogWrite")
            arg_DataFrame.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(arg_Table)
            self._ReturnStatus = True
        except Exception as ExceptionError:
            self._Exception = ExceptionError
        return self._ReturnStatus
    
    def write_Database (
            self,
            arg_Source,
            arg_DBServer,
            arg_DBName,
            arg_DBUser,
            arg_DBPass,
            arg_DBTable,
            arg_Config
        ):
        # ======== Set error message that will be shown
        self.reset_Status("FailedDBWrite")
        import time
        arg_Source = arg_Source.repartition(arg_Config["Partition"])
        vRetryMaximum = arg_Config["Retry"]["Maximum"]
        vRetryDelay = arg_Config["Retry"]["Delay"]
        # ======== Retry until it reached the maximum
        for vAttempt in range(vRetryMaximum):
            try:
                self._ReturnValue = arg_Source.write.format(arg_Config["Format"]) \
                    .mode(arg_Config["Write"]["Mode"]) \
                    .option("batchsize", arg_Config["Batch"]) \
                    .option("driver", arg_Config["Driver"]) \
                    .option("url", f'{arg_Config["Format"]}:{arg_Config["Type"].lower()}://{arg_DBServer}:{arg_Config["Port"]};database={arg_DBName};user={arg_DBUser};password={arg_DBPass}') \
                    .option("dbtable", arg_DBTable) \
                    .option("user", arg_DBUser) \
                    .option("password", arg_DBPass) \
                    .save()
                self._ReturnStatus = True
                break  # Exit the loop if successful

            except Exception as ExceptionError:
                self._Exception = ExceptionError
                # ======== Show the error for each retry
                self.show_Info("DB Error", self.clean_Exception(), "red")
                self.show_Info("Target", f"{arg_DBTable} ::> Retrying in {vRetryDelay} seconds ({vAttempt+1} of {vRetryMaximum})")
                if vAttempt < vRetryMaximum - 1:  # Check if more retries are left
                    time.sleep(vRetryDelay)
                    self._ReturnStatus = False
        return self._ReturnStatus
        
    def call_GA4runReport (
            self,
            arg_Client, 
            arg_PropertyID, 
            arg_StartDate, 
            arg_EndDate, 
            arg_Dimensions, 
            arg_Metrics,
            arg_Offset,
            arg_Limit,
            arg_Filter = None
        ):
        try:
            self.reset_Status("FailedGA4Report")
            from google.analytics.data_v1beta.types import (
                Filter,
                Metric,
                Dimension, 
                DateRange, 
                RunReportRequest,
                FilterExpression
            )
            # ======== Create the request credential
            vRequestParam = {
                "property": f"properties/{arg_PropertyID}",
                "dimensions": [
                    Dimension(
                        name=col_dimension
                    ) for col_dimension in arg_Dimensions
                ],
                "metrics": [
                    Metric(
                        name=col_metric
                    ) for col_metric in arg_Metrics
                ],
                "date_ranges": [
                    DateRange(
                        start_date=arg_StartDate, 
                        end_date=arg_EndDate
                    )
                ],
                "limit": arg_Limit,
                "offset": arg_Offset
            }
            if arg_Filter and arg_Filter is not None:
                for vFilterKey, vFilterConditions in arg_Filter.items():
                    for vFieldName, vFieldValues in vFilterConditions.items():
                        vFilterValues = []
                        for vFieldValue in vFieldValues:
                            vFilterValues.append(vFieldValue)
                        # ======== Check for Filter Key
                        match vFilterKey:
                            case "DimensionInList":
                                vRequestParam["dimension_filter"] = FilterExpression(
                                    filter=Filter(
                                        field_name=vFieldName,
                                        in_list_filter=Filter.InListFilter(
                                            values=[str(vFilterValue) for vFilterValue in vFilterValues]
                                        )
                                    )
                                )
                            case _:
                                pass
            retVal = arg_Client.run_report(
                RunReportRequest(**vRequestParam)
            )
            if retVal:
                self._ReturnValue = retVal
                self._ReturnStatus = True
        except Exception as ExceptionError:
            self._Exception = ExceptionError
        return self._ReturnStatus

    def pull_GA4API (
            self,
            arg_Credentials,
            arg_PropertyID, 
            arg_StartDate, 
            arg_EndDate, 
            arg_Dimensions, 
            arg_Metrics,
            arg_Offset,
            arg_Limit,
            arg_Filter = None
        ):
        try:
            # ======== Set error message that will be shown
            self.reset_Status("FailedGA4RowCount")
            from google.oauth2 import (
                service_account
            )
            from google.analytics.data_v1beta import (
                BetaAnalyticsDataClient
            )
            vCredentials = service_account.Credentials.from_service_account_info(
                arg_Credentials
            )
            resp_getGA4Client = BetaAnalyticsDataClient(
                credentials=vCredentials
            )
            import time
            vOffset = arg_Offset
            vLimit = arg_Limit
            # ======== Fetch GA4 API and get the initial row count
            self.call_GA4runReport(
                resp_getGA4Client, 
                arg_PropertyID, 
                arg_StartDate, 
                arg_EndDate, 
                arg_Dimensions, 
                arg_Metrics, 
                1, 
                1,
                arg_Filter
            )
            time.sleep(0.5)
            if self._ReturnStatus is True:
                retVal = self._ReturnValue
                # ======== Set error message that will be shown
                self.reset_Status("FailedGA4Pull")
                vColumns = [h.name for h in list(retVal.dimension_headers) + list(retVal.metric_headers)]
                # ======== Save the row count
                self._TotalRows = retVal.row_count
                vTotalRows = format(self._TotalRows, ',')
                if self._TotalRows == 0:
                    raise Exception("No rows found")
                else:
                    self.show_Info("Fetched", f"GA4 Total Count ({vTotalRows} rows) ::> Successful")
                    vData = []
                    is_Success = False
                    while vOffset < self._TotalRows:
                        time.sleep(0.5)
                        # ======== Fetch API per batch
                        self.call_GA4runReport(
                            resp_getGA4Client, 
                            arg_PropertyID, 
                            arg_StartDate, 
                            arg_EndDate, 
                            arg_Dimensions, 
                            arg_Metrics, 
                            vOffset, 
                            vLimit,
                            arg_Filter
                        )
                        if self._ReturnStatus is True:
                            is_Success = True
                            retVal = self._ReturnValue
                            self.reset_Status("FailedGA4Pull")
                            vRows = [[dv.value for dv in row.dimension_values] + [mv.value for mv in row.metric_values] for row in retVal.rows]
                            vData.extend(vRows)
                            vOffset += vLimit
                            vOffsetRows = format(min(vOffset, self._TotalRows), ',')
                            # self._TotalColumns = len(retVal.dimension_headers) + len(retVal.metric_headers)
                            self.show_Info("Processed", f"{vOffsetRows} rows of {vTotalRows} rows")
                    if is_Success is True:
                        retVal = spark.createDataFrame(vData, schema=vColumns)
                        if retVal:
                            self._TotalColumns = len(vColumns)
                            self._ColumnNames = retVal.columns
                            self._ReturnValue = retVal
                            self._ReturnStatus = True
        except Exception as ExceptionError:
            self._Exception = ExceptionError
        return self._ReturnStatus
    
    def is_ColumnExists (
            self,
            arg_DataFrame,
            arg_ColumnName
        ):
        # ======== Check if the column exists in the DataFrame
        is_Exists = False
        try:
            if arg_ColumnName in arg_DataFrame.columns:
                is_Exists = True
        except Exception as ExceptionError:
            self._Exception = ExceptionError
        return is_Exists

    def set_SparkType (
            self,
            arg_Type = "string"
        ):
        try:
            # ======== Set error message that will be shown
            self.reset_Status("FailedTypeSet")
            from pyspark.sql.types import (
                StringType, 
                IntegerType, # 32-bit signed integer; e.g., 42
                LongType, # 64-bit signed integer; e.g., 1234567890123
                FloatType, # 32-bit single-precision floating point number; e.g., 3.14
                DoubleType, # 64-bit double-precision floating point number; e.g., 2.718281828459045
                BooleanType, # boolean value; e.g., True or False
                ByteType, # 8-bit signed integer; e.g., 127
                ShortType, # 16-bit signed integer; e.g., 32767
                BinaryType, # byte array; e.g., b'\x00\x01'
                DateType, #  date without a time zone; e.g., date(2023, 10, 5)
                TimestampType, # timestamp without a time zone; e.g., timestamp('2023-10-05 12:34:56')
                TimestampNTZType, # timestamp without a time zone; e.g., timestamp('2023-10-05 12:34:56')
                ArrayType, # array of elements of a specified data type; e.g., [1, 2, 3]
                MapType, # map of key-value pairs, with specified data types for keys and values; e.g., {"key1": 1, "key2": 2}
                StructType, # complex data type with multiple fields (like a row in a table)
                StructField, # single field in a StructType
                NullType # null value. Typically used internally and not explicitly
            )
            # ======== Set the correct data type
            match arg_Type:
                case "integer":
                    self._ReturnValue = IntegerType()

                case "long":
                    self._ReturnValue = LongType()
                
                case "float":
                    self._ReturnValue = FloatType()

                case "double":
                    self._ReturnValue = DoubleType()
                
                case "boolean":
                    self._ReturnValue = BooleanType()
                
                case "byte":
                    self._ReturnValue = ByteType()
                
                case "short":
                    self._ReturnValue = ShortType()
                
                case "binary":
                    self._ReturnValue = BinaryType()
                
                case "date":
                    self._ReturnValue = DateType()
                
                case "timestamp":
                    self._ReturnValue = TimestampType()
                
                case "timestamp_ntz":
                    self._ReturnValue = TimestampNTZType()
                
                case "array":
                    self._ReturnValue = ArrayType()
                
                case "map":
                    self._ReturnValue = MapType()
                
                case "struct":
                    self._ReturnValue = StructType()
                
                case "struct_field":
                    self._ReturnValue = StructField()
                
                case "null":
                    self._ReturnValue = NullType()

                case "string":
                    self._ReturnValue = StringType()

                case _:
                    self._ReturnValue = StringType()
            self._ReturnStatus = True
            return self._ReturnValue
        except Exception as ExceptionError:
            self._Exception = ExceptionError
        return False

    def log_Cloud (
            self,
            arg_KeyAPI,
            arg_SiteName,
            arg_Environment,
            arg_Payload
        ):
        import os
        from datadog_api_client import ApiClient, Configuration
        from datadog_api_client.v2.api.logs_api import LogsApi
        from datadog_api_client.v2.model.content_encoding import ContentEncoding
        from datadog_api_client.v2.model.http_log import HTTPLog
        from datadog_api_client.v2.model.http_log_item import HTTPLogItem
        HTTP_Response = None
        try:
            # ======== Set the credentials on the environment
            os.environ['DD_API_KEY'] = arg_KeyAPI
            os.environ['DD_SITE'] = arg_SiteName
            os.environ['ENV'] = arg_Environment
            Request_Body = HTTPLog(
                [
                    HTTPLogItem(**arg_Payload)
                ]
            )
            DD_Config = Configuration()
            with ApiClient(DD_Config) as API_Client:
                DD_Instance = LogsApi(API_Client)
                # ======== Transfer the Payload to HTTP Request Body
                HTTP_Response = DD_Instance.submit_log(body=Request_Body)
        except Exception as ExceptionError:
            self._Exception = ExceptionError
        return HTTP_Response
    
    def log_File (
            self,
            arg_LoggerName,
            arg_LogOutput,
            arg_LogFileName = None
        ):
        Logger = None
        try:
            import os
            import logging
            # ======== Create a logger
            vLogger = logging.getLogger(arg_LoggerName)
            if vLogger.hasHandlers():
                vLogger.handlers.clear()
            # ======== Capture all types of log messages
            vLogger.setLevel(logging.DEBUG)
            # ======== Create a file handler for logging to a file in the current directory
            vlogCurrdir = os.getcwd()
            if arg_LogFileName is not None:
                vlogFilePath = arg_LogFileName
            else:
                vlogFilePath = f"{vlogCurrdir}/Logs/{arg_LoggerName}.log"
            if os.path.exists(file_path) is False:
                with open(vlogFilePath, 'w') as log_file:
                    pass
            vlogHandler = logging.FileHandler(vlogFilePath, mode='w')
            # ======== Add the file handler to the logger
            vLogger.addHandler(vlogHandler)
            # Handle exclusively without passing to ancestor loggers
            vLogger.propagate = False
            # ======== Create a log
            vLogger.info(arg_LogOutput)
        except Exception as ExceptionError:
            self._Exception = ExceptionError
        return vLogger
    
    def log_Success (
            self,
            arg_Response,
            arg_Caller,
            arg_Module,
            arg_Version,
            arg_Datahouse,
            arg_DBUtils
        ):
        try:
            # ======== Log the Success Details
            from datetime import datetime
            vFinished = datetime.now()
            vStarted = self._Started
            vDuration = vFinished - vStarted
            vStarted = self.get_PHdatetime(vStarted)
            vFinished = self.get_PHdatetime(vFinished)
            vLogCloud = {}
            vLogFile =  f"     Caller ::> {arg_Caller}\n"
            vLogFile += f"     Module ::> {arg_Module}\n"
            vLogFile += f"    Version ::> {arg_Version}\n"
            vLogFile += f"  Datahouse ::> {arg_Datahouse}\n"
            vLogFile += f"Environment ::> {self._Environment}\n"
            vLogFile += f"    Started ::> {vStarted}\n"
            vLogFile += f"   Finished ::> {vFinished}\n"
            vLogFile += f" Total Rows ::> {self._TotalRows}\n"
            vLogFile += f" Total Cols ::> {self._TotalColumns}\n"
            vLogFile += f"   Duration ::> {vDuration}\n"
            vRunner = arg_DBUtils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
            if vRunner:
                vLogFile += f"     Runner ::> {vRunner}\n"
            # ======== Send to File Log
            if self.LibConfig["Main"]["Switchboard"]["FileLog"] is True:
                self.log_File(
                        f"{arg_Module}-Success", 
                        vLogFile
                    )
            # ======== Send to Cloud Log
            if self.LibConfig["Main"]["Switchboard"]["CloudLog"] is True:
                vLogCloud["ddsource"] = arg_Module
                vLogCloud["hostname"] = f'{self.AppLib["Metadata"]["Repository"]}'
                vLogCloud["message"] = f'[SUCCESS] {arg_Datahouse} ({self._Environment}) - {arg_Caller} - {arg_Response["Schema"]["Rows"]["NetTotal"]} rows'
                vLogCloud["status"] = "success"
                vLogCloud["Caller"] = f"{arg_Caller}"
                vResponse = self.fix_Dictionary(arg_Response)
                vLogCloud["Response"] = vResponse
                vLogCloud["service"] = dbutils.secrets.get (
                        self.LibConfig["Main"]["Provider"]["Datadog"]["ServiceName"]["Scope"],
                        self.LibConfig["Main"]["Provider"]["Datadog"]["ServiceName"]["Key"]
                    )
                self.log_Cloud (
                    dbutils.secrets.get (
                        self.LibConfig["Main"]["Provider"]["Datadog"]["KeyAPI"]["Scope"],
                        self.LibConfig["Main"]["Provider"]["Datadog"]["KeyAPI"]["Key"]
                    ),
                    self.LibConfig["Main"]["Provider"]["Datadog"]["SiteName"],
                    self._Environment,
                    vLogCloud
                )
        except Exception as ExceptionError:
            self._Exception = ExceptionError
        return
    
    def fix_Dictionary (
            self,
            arg_Dict
        ):
        # ======== Make the dictionary print friendly
        try:
            if isinstance(arg_Dict, dict):
                return {key: self.fix_Dictionary(value) for key, value in arg_Dict.items()}
            elif isinstance(arg_Dict, list):
                return [self.fix_Dictionary(item) for item in arg_Dict]
            else:
                return str(arg_Dict)
        except Exception as ExceptionError:
            self._Exception = ExceptionError
        return False

    def clean_Exception (
            self,
            arg_Exception = None
        ):
        # ======== Show only the important Exception message
        try:
            if arg_Exception is None:
                vException = str(self._Exception)
            else:
                vException = str(arg_Exception)
            vPosition = vException.find("JVM stacktrace")
            if vPosition != -1:
                vException = vException[:vPosition]
            else:
                vException = vException[:2000]
            vException = vException.split('\n')[0].split('\r')[0]
            return vException
        except Exception as ExceptionError:
            self._Exception = ExceptionError
        return False
    
    def show_Exception (
            self,
            arg_Cause,
            arg_DBUtils = None,
            arg_Module = None,
            arg_Version = None,
            arg_Datahouse = None,
            arg_Caller = None,
            arg_Response = None,
            arg_Line = None,
            arg_Func = None,
            arg_Code = None
        ):
        try:
            # ======== Display the Exception Message
            vLogFile = f"     Module ::> {arg_Module}\n"
            vLogFile += f"    Version ::> {arg_Version}\n"
            vLogFile += f"  Datahouse ::> {arg_Datahouse}\n"
            vLogFile += f"Environment ::> {self._Environment}\n"
            if arg_Cause is not None:
                vLogScreen = f"  Exception ::> {self.clean_Exception(arg_Cause)}"
                vLogFile += f"{vLogScreen}\n"
            else:
                vLogScreen = "\n"
            self.show_Text(f"{vLogScreen}", "red")
            if arg_Line is not None:
                vLogScreen = f"       Line ::> {arg_Line}"
                vLogFile += f"{vLogScreen}\n"
                self.show_Text(vLogScreen, "red")
            if arg_Func is not None:
                vLogScreen = f"   Function ::> {arg_Func}"
                vLogFile += f"{vLogScreen}\n"
                self.show_Text(vLogScreen, "red")
            if arg_Code is not None:
                vLogScreen = f"       Code ::> {arg_Code}"
                vLogFile += f"{vLogScreen}\n"
                self.show_Text(vLogScreen, "red")
            # ======== Get the notebook path
            vFilename = arg_DBUtils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
            if vFilename:
                vLogScreen = f"   Filename ::> {vFilename}"
                vLogFile += f"{vLogScreen}\n"
                self.show_Text(vLogScreen, "red")
            vLogFile +=  f"     Caller ::> {arg_Caller}\n"
            # ======== Get the username of the person who run this
            vRunner = arg_DBUtils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
            if vRunner:
                vLogScreen = f"     Runner ::> {vRunner}"
                vLogFile += f"{vLogScreen}\n"
                self.show_Text(vLogScreen, "red")
            # ======== Get PH Date & Time
            vDateTimeFormatted = self.get_PHdatetime()
            vLogScreen = f"  Timestamp ::> {vDateTimeFormatted}\n"
            vLogFile += f"{vLogScreen}\n"
            self.show_Text(vLogScreen, "red")
            # ======== Send to File Log
            if self.LibConfig["Main"]["Switchboard"]["FileLog"] is True:
                self.log_File(
                        f"{arg_Module}-Failed", 
                        vLogFile
                    )
            # ======== Send to Cloud Log
            vLogCloud = {}
            if self.LibConfig["Main"]["Switchboard"]["CloudLog"] is True:
                vLogCloud["ddsource"] = arg_Module
                vLogCloud["hostname"] = f'{self.AppLib["Metadata"]["Repository"]}'
                vLogCloud["message"] = f"[ERROR] {arg_Datahouse} ({self._Environment}) - {arg_Caller} - {arg_Module} (Function:{arg_Func})"
                vLogCloud["status"] = "error"
                vLogCloud["Caller"] = f"{arg_Caller}"
                if arg_Response is not None:
                    vResponse = self.fix_Dictionary(arg_Response)
                    vLogCloud["Response"] = vResponse
                vLogCloud["service"] = dbutils.secrets.get (
                        self.LibConfig["Main"]["Provider"]["Datadog"]["ServiceName"]["Scope"],
                        self.LibConfig["Main"]["Provider"]["Datadog"]["ServiceName"]["Key"]
                    )
                self.log_Cloud (
                    dbutils.secrets.get (
                        self.LibConfig["Main"]["Provider"]["Datadog"]["KeyAPI"]["Scope"],
                        self.LibConfig["Main"]["Provider"]["Datadog"]["KeyAPI"]["Key"]
                    ),
                    self.LibConfig["Main"]["Provider"]["Datadog"]["SiteName"],
                    self._Environment,
                    vLogCloud
                )
        except Exception as ExceptionError:
            self._Exception = ExceptionError
        return
    
    def show_ErrorMsg (
            self,
            arg_ErrorKey = None,
            arg_HeadTail = None,
            arg_BodyTail = None
        ):
        try:
            vLine1 = None
            if arg_ErrorKey is None:
                if self._ErrorKey:
                    vErrorKey = self._ErrorKey
                else:    
                    vErrorKey = None
            else:
                vErrorKey = arg_ErrorKey
            if arg_HeadTail is None:
                if self._ErrorTail:
                    vHeadTail = self._ErrorTail
                else:
                    vHeadTail = None
            else:
                vHeadTail = arg_HeadTail
            vBodyTail = arg_BodyTail
            vLine2 = None
            if vErrorKey is not None and vErrorKey in self.LibConfig["Main"]["Error"]["Message"]:
                if "Head" in self.LibConfig["Main"]["Error"]["Message"][vErrorKey]:
                    if vHeadTail is None:
                        if self._Exception is None:
                            vLine1 = f'{self.LibConfig["Main"]["Error"]["Message"][vErrorKey]["Head"]}'
                        else:
                            vLine1 = f'{self.LibConfig["Main"]["Error"]["Message"][vErrorKey]["Head"]} ::> {self.clean_Exception()}'
                    else:
                        vLine1 = f'{self.LibConfig["Main"]["Error"]["Message"][vErrorKey]["Head"]} ::> {vHeadTail}'
                    if "Body" in self.LibConfig["Main"]["Error"]["Message"][vErrorKey]:
                        if vBodyTail is None:
                            vLine2 = f'{self.LibConfig["Main"]["Error"]["Message"][vErrorKey]["Body"]}'
                        else:
                            vLine2 = f'{self.LibConfig["Main"]["Error"]["Message"][vErrorKey]["Body"]} ::> {vBodyTail}'
            if vLine1 is None:
                # ======== Programmer Error - Error Key is not set
                retVal = 'An Unknown Error Occurred'
            else:
                if vLine2 is None:
                    retVal = vLine1
                else:
                    retVal = f'{vLine1}\n{vLine2}'
            return retVal
        except Exception as ExceptionError:
            self._Exception = ExceptionError
        return False
    
    def decrypt_Data (
            self, 
            arg_Salt,
            arg_EncryptedData, 
            arg_Passphrase
        ):
        try:
            # ======== Set error message that will be shown
            self.reset_Status("FailedDecryption")
            import json
            import os
            from cryptography.fernet import Fernet
            from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
            from cryptography.hazmat.primitives import hashes
            import base64
            var_Salt = base64.urlsafe_b64decode(arg_Salt)
            var_KDF = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=var_Salt,
                iterations=100000,
            )
            var_Passphrase = arg_Passphrase.encode()
            var_Key = base64.urlsafe_b64encode(var_KDF.derive(var_Passphrase))
            var_Cipher = Fernet(var_Key)
            var_DecryptedData = var_Cipher.decrypt(arg_EncryptedData)
            var_DecryptedString = var_DecryptedData.decode()
            self._ReturnValue = json.loads(var_DecryptedString)
            self._ReturnStatus = True
        except Exception as ExceptionError:
            self._Exception = ExceptionError
        return self._ReturnStatus

    def get_MediaPath (
            self,
            arg_MediaParent,
            arg_MediaType,
            arg_MediaClass,
            arg_DataLayer,
            arg_SubDir = None,
            arg_Extension = None
        ):
        try:
            from datetime import datetime
            vNow = datetime.now()
            vYear = vNow.year
            vMonth = vNow.strftime("%m")
            vDay = vNow.strftime("%d")
            # Google/GA4/RZ/GA4-Page_Test-RZ
            vBaseDir = f"{arg_MediaParent}/{arg_MediaType}/{arg_DataLayer}/{arg_MediaType}-{arg_MediaClass}-{arg_DataLayer}"
            vMediaDir = vBaseDir
            # ======== Set the correct Media Path
            match arg_SubDir:
                case "y/m/d": # 2024/01/01
                    vMediaDir = vBaseDir + f"/{vYear}/{vMonth}/{vDay}"

                case "y/m-d": # 2024/01-01
                    vMediaDir = vBaseDir + f"/{vYear}/{vMonth}-{vDay}"

                case "y-m-d": # 2024-01-01
                    vMediaDir = vBaseDir + f"/{vYear}-{vMonth}-{vDay}"

                case "Clone": # Google/UA/RZ/_Clone/Org_Blog/Campaigns
                    vMediaDir = f"{arg_MediaParent}/{arg_MediaType}/{arg_DataLayer}/_Clone/{arg_MediaClass}"

                case "Custom": # Google/_Custom/Org_Blog/Campaigns
                    vMediaDir = f"{arg_MediaParent}/_Custom/{arg_MediaClass}"

                case _:
                    vMediaDir = vBaseDir
            if arg_Extension is not None:
                vMediaDir = vMediaDir + f"/{arg_Extension}"
            return vMediaDir
        except Exception as ExceptionError:
            self._Exception = ExceptionError
        return False

    def get_PHdatetime (
            self,
            arg_Convert = None
        ):
        try:
            from datetime import datetime, timedelta
            if arg_Convert is None:
                # ======== Get the current date and time
                vDateTime = datetime.now()
            else:
                vDateTime = arg_Convert
            # ======== Get PH date and time
            vGMT8 = vDateTime + timedelta(hours=8)
            return vGMT8.strftime("%Y-%m-%d %I:%M:%S %p")
        except Exception as ExceptionError:
            self._Exception = ExceptionError
        return False

    def show_Title (
            self,
            arg_Title
        ):
        # ======== Set the correctly-adjusted Title
        try:
            vWidth = 11
            vTitleLen = len(arg_Title)
            if vTitleLen < vWidth:
                vTitleSpaces = vWidth - vTitleLen
                vTitle = " " * vTitleSpaces + arg_Title
            else:
                vTitle = arg_Title 
            return vTitle + " ::> "
        except Exception as ExceptionError:
            self._Exception = ExceptionError
        return False

    def show_Info (
            self,
            arg_Title,
            arg_Info,
            arg_Color = "white"
        ):
        # ======== Show information with correctly-adjusted title and color
        try:
            self.show_Text(f"{self.show_Title(arg_Title)}{arg_Info}", arg_Color)
            return True
        except Exception as ExceptionError:
            self._Exception = ExceptionError
        return False

    def show_Text (
            self,
            arg_Text, 
            arg_Color = "white"
        ):
        # ======== Show text in colors
        try:
            vColorTable = {
                "red": "\033[91m",
                "green": "\033[92m",
                "yellow": "\033[93m",
                "blue": "\033[94m",
                "magenta": "\033[95m",
                "cyan": "\033[96m",
                "white": "\033[97m",
                "reset": "\033[0m"
            }
            vColorCode = vColorTable.get(arg_Color, vColorTable["reset"])
            vText = f"{vColorCode}{arg_Text}{vColorTable['reset']}"
            if self.LibConfig["Main"]["Switchboard"]["SilentMode"] is False:
                print(vText)
            return vText
        except Exception as ExceptionError:
            self._Exception = ExceptionError
        return False

    def Debug (
            self, 
            arg_Value,
            argText = False
        ):
        # ======== Show a debug message
        try:
            if self.LibConfig["Main"]["Switchboard"]["DebugMode"] is True:
                vCounter = self._DebugCounter
                vCounter = vCounter + 1
                self._DebugCounter = vCounter
                import traceback
                stack = traceback.extract_stack()
                stack_file, stack_line, stack_func, stack_text = stack[-2]
                if argText is False:
                    vMarker = f'[DEBUG] ::> {stack_func}'
                else:
                    vMarker = f'[DEBUG] ::> {argText}'
                self.show_Text(f'[{str(vCounter).zfill(4)}::{str(stack_line).zfill(4)}]{vMarker}\n{arg_Value}\r\n', "yellow")
                return True
            else:
                return False
        except Exception as ExceptionError:
            self._Exception = ExceptionError
        return False

class MigrationError(Exception):
    # ======== Custom Error Exception for Data_Migration class
    pass
