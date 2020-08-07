import GeoIDLookUp.geoID
import Decryption.Decryption
import Decryption.encrypt
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.functions.typedLit
import org.apache.spark.sql.functions.{from_json, to_json, udf}
import org.apache.spark.sql.types.{DataTypes, StringType, StructType}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions.regexp_replace
import play.api.libs.json.JsValue

 val conf = ConfigFactory.load.getConfig("dev")

    //The Sparksession object 12is the entry point of the spark program and this allows for creation of dataframe

    val spark = SparkSession.builder().appName("The final payload").master(conf.getString("execution.mode")).getOrCreate

    //Import spark implicits so that we can use the '$' operator

    import spark.implicits._


    //Prevents INFO messages from being displayed

    spark.sparkContext.setLogLevel("ERROR")

    //Configure the number of partitions that are used when shuffling data for joins or aggregations

    spark.conf.set("spark.sql.shuffle.partitions", "2")


    //Read the messages from the Kafka topic in the form of a byte stream

     val inputDf = spark.read.format("kafka").option("kafka.bootstrap.servers", conf.getString("bootstrap.servers")).option("subscribe", "PSAP-EV7Z7-P6YWB").option("startingOffsets", """{"PSAP-EV7Z7-P6YWB":{"0":165411}}""").option("endingOffsets","""{"PSAP-EV7Z7-P6YWB":{"0":175816}}""").load()


    //val schema9 = ScalaReflectionException.sc

    //Convert the byte stream to String

    val payloadJsonDf = inputDf.selectExpr("CAST(value AS STRING)")

    //Create schema for the JSON message

    val schema = new StructType().add("gateway", DataTypes.StringType).add("key", DataTypes.StringType).add("timestamp", DataTypes.StringType).add("payload", DataTypes.StringType)

    //Apply schema to the JSON message to create a Dataframe

    val payloadNestedDf = payloadJsonDf.select(from_json($"value",schema).as("PL"))

    //We flatten the dataframe because it consists of nested columns

    val payloadFlattenedDf = payloadNestedDf.selectExpr("PL.gateway","PL.key","PL.timestamp","PL.payload")

    //Create User defined function for Decrypt and Encrypt   and GeoLookUp

    val GeoUDF = udf[List[Int],String,String](geoID.geoIDFind)


   val DecryptUDF = udf[String,String](Decryption.decrypt)


    val EncryptUDF = udf[String,String](Decryption.encrypt)


    //Apply the decryption function to the payload column. The resulting column is called "UDFPayload"

   val newDecryptDf = payloadFlattenedDf.select(DecryptUDF(payloadFlattenedDf("payload")))

    //Rename Resulting Column as payload

   val finalDecryptDf = newDecryptDf.select($"UDF(payload)".alias("payload"))

    //Define the schema for the decrypted payload

  //val schema1 = new StructType().add("preamble",DataTypes.StringType).add("incidentCode",DataTypes.StringType).add("incidentMessage",DataTypes.StringType).add("raw",DataTypes.StringType)

    val schema1 = new StructType()
      .add("preamble", new StructType()
        .add("uuid",DataTypes.StringType)
          .add("psap",DataTypes.StringType)
          .add("gateway", DataTypes.StringType)
          .add("authorizationKey",DataTypes.StringType)
          .add("version",DataTypes.StringType)
          .add("timestamp",DataTypes.StringType))
      .add("incidentCode",DataTypes.StringType)
      .add("incidentMessage",new StructType()
          .add("ReceivedTime",DataTypes.StringType)
            .add("CallingPhoneNumber",DataTypes.StringType)
              .add("CallNumber",DataTypes.StringType)
          .add("AgencyID",DataTypes.StringType)
        .add("AgencyDescription",DataTypes.StringType)
        .add("DisciplineID",DataTypes.StringType)
        .add("DisciplineDescription",DataTypes.StringType)
        .add("NatureCode",DataTypes.StringType)
        .add("NatureDescription",DataTypes.StringType)
        .add("SituationDisciplineID",DataTypes.StringType)
        .add("SituationDisciplineDescription",DataTypes.StringType)
        .add("SituationCode",DataTypes.StringType)
        .add("SituationDescription",DataTypes.StringType)
        .add("AddressLocationType",DataTypes.StringType)
        .add("AddressPlaceName",DataTypes.StringType)
        .add("AddressStreetNumber",DataTypes.StringType)
        .add("AddressStreetAddress",DataTypes.StringType)
        .add("AddressApartmentNumber",DataTypes.StringType)
        .add("AddressFloor",DataTypes.StringType)
        .add("AddressLocationInformation",DataTypes.StringType)
        .add("AddressLandmark",DataTypes.StringType)
        .add("AddressCity",DataTypes.StringType)
        .add("AddressState",DataTypes.StringType)
        .add("AddressZipCode",DataTypes.StringType)
        .add("AddressZipCodePlus4",DataTypes.StringType)
        .add("AddressCounty",DataTypes.StringType)
        .add("AddressLatitude",DataTypes.StringType)
        .add("AddressLongitude",DataTypes.StringType)
        .add("AddressOverridden",DataTypes.StringType)
        .add("BoxCard",DataTypes.StringType)
        .add("AlarmNumber",DataTypes.StringType)
        .add("ReasonTypeCode",DataTypes.StringType)
        .add("Notes",DataTypes.StringType)
        .add("UserDefinedFields",DataTypes.StringType)
        .add("InFrom",DataTypes.StringType)
        .add("StatusCode",DataTypes.StringType)
        .add("Cancelled",DataTypes.StringType)
        .add("IsArchived",DataTypes.StringType)
        .add("RemovedFromHistory",DataTypes.StringType)
        .add("PriorityCode",DataTypes.StringType)
        .add("PriorityDescription",DataTypes.StringType)
        .add("AgencyInCode",DataTypes.StringType)
        .add("ReportingPartyName",DataTypes.StringType)
        .add("ReportingPartyAddress",DataTypes.StringType)
        .add("ReportingPartyPhone",DataTypes.StringType)
        .add("PSAPFix",DataTypes.StringType)
        .add("LastStatusTime",DataTypes.StringType)
        .add("ScheduledTime",DataTypes.StringType)
        .add("ReminderTime",DataTypes.StringType)
        .add("DispatcherID",DataTypes.StringType)
        .add("CallTakerID",DataTypes.StringType)
        .add("LinkCallNumber",DataTypes.StringType)
        .add("LinkType",DataTypes.StringType)
        .add("NoUnitAvailable",DataTypes.StringType)
        .add("Reoccuring",DataTypes.StringType)
        .add("Reassigned",DataTypes.StringType)
        .add("CancellationNarrative",DataTypes.StringType)
        .add("ExternalKey",DataTypes.StringType)
        .add("ExternalSource",DataTypes.StringType)
        .add("IncidentTime",DataTypes.StringType)
        .add("CallZone",DataTypes.StringType)     )
      .add("raw",DataTypes.StringType)

          
   val finalResult =   finalDecryptDf.withColumn("FinalFrame",from_json($"payload",schema1)).select($"FinalFrame.*")

finalResult.write.format("json").save("/home/temp/Desktop/BatchExtractionSpark3")

