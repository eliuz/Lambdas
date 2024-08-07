import json
import xml.etree.ElementTree as ET
import csv
import boto3
import json

rs3 = boto3.resource("s3")
s3 = boto3.client("s3")


def lambda_handler(event, context):

    bucketname = "[TU_BUCKET]"
    bucket = rs3.Bucket("[TU_BUCKET]")
    prefix = "public/Antena3/series"
    result = s3.list_objects_v2(Bucket=bucketname, Prefix=prefix)
    resultCap = rs3.Object(bucketname, "public/Antena3/csv/series/Capitulos1.csv")
    resultCap.download_file("/tmp/Capitulos1.csv")
    resultGen = rs3.Object(bucketname, "public/Antena3/csv/series/Genericos1.csv")
    resultGen.download_file("/tmp/Genericos1.csv")

    with open("/tmp/Capitulos1.csv", "w+", encoding="utf8", newline="") as csvfile:
        write = csv.writer(csvfile, delimiter="|")
        write.writerow(
            [
                "asset_id ",
                " season_id ",
                " toolboxId ",
                " episode_name ",
                " season ",
                " episode_number ",
                " summary_short ",
                " rating ",
                " run_time ",
                " display_run_time ",
                " year ",
                " actors ",
                " genre ",
                " start_date ",
                " end_date ",
                " wallpaper1 ",
                " wallpaper2",
            ]
        )
        for obj in result["Contents"]:
            file = obj["Key"]
            start = file.find("/")
            name = file[start + 1 :]

            if file.endswith(".xml"):
                objct = rs3.Object(bucketname, file)
                data = objct.get()["Body"].read().decode("ISO-8859-1")
                root = ET.fromstring(data)
                g = root[0][0]  # se cambia primer valor a 0 y se borra el 3

                sId = g.get("Asset_ID")[:-8]
                oId = g.get("Asset_ID")
                title = g.get("Asset_Name")
                end = title.find("_")
                title = title[:end]
                prov = g.get("Provider")

                for appd in root.iter("App_Data"):

                    if appd.get("Name") == "Title_Brief":
                        tb = appd.get("Value")

                    if appd.get("Name") == "Director":
                        director = appd.get("Value")

                    if appd.get("Name") == "Type":
                        typ = appd.get("Value")

                    if appd.get("Name") == "Season_Number":
                        seas = appd.get("Value")
                        seasId = sId + seas

                    if appd.get("Name") == "Summary_Short":
                        summ = appd.get("Value")

                    if appd.get("Name") == "Studio":
                        stud = appd.get("Value")

                    if appd.get("Name") == "Episode_Name":
                        epName = appd.get("Value")
                        end = epName.find("T0")
                        epName = epName[:end]

                    if appd.get("Name") == "Episode_Number":
                        epNumber = appd.get("Value")

                    if appd.get("Name") == "Rating":
                        rating = appd.get("Value")

                    if appd.get("Name") == "Run_Time":
                        run = appd.get("Value")

                    if appd.get("Name") == "Display_Run_Time":
                        displayRun = appd.get("Value")

                    if appd.get("Name") == "Year":
                        year = appd.get("Value")

                    if appd.get("Name") == "Actors_Display":
                        actors = appd.get("Value")

                    if appd.get("Name") == "Genre":
                        genre = appd.get("Value")

                    if appd.get("Name") == "Licensing_Window_End":
                        eDate = appd.get("Value")

                    if appd.get("Name") == "Licensing_Window_Start":
                        sDate = appd.get("Value")

                print(oId)
                toolboxId = "urn:tve:a3series"

                write.writerow(
                    [
                        oId,
                        seasId,
                        toolboxId,
                        epName,
                        int(seas),
                        int(epNumber),
                        summ,
                        rating,
                        run,
                        displayRun,
                        int(year),
                        actors,
                        genre,
                        sDate,
                        eDate,
                        "https://s3.amazonaws.com/[TU_BUCKET]/public/Antena3/Imagenes-Series/"
                        + sId
                        + "/"
                        + sId
                        + "_highlight1.jpg",
                        "https://s3.amazonaws.com/[TU_BUCKET]/public/Antena3/Imagenes-Series/"
                        + sId
                        + "/"
                        + sId
                        + "_highlight2.jpg",
                    ]
                )

    bucket.upload_file(
        "/tmp/Capitulos1.csv", "public/Antena3/csv/series/Capitulos1.csv"
    )

    with open("/tmp/Genericos1.csv", "w", encoding="utf8", newline="") as csvfile:

        write = csv.writer(csvfile, delimiter="|")
        write.writerow(
            [
                "asset_id",
                "asset_name",
                "provider",
                "spanish_title",
                "english_title",
                "original_title",
                "title_brief",
                "alternative_title",
                "summary_long",
                "summary_short",
                "writer_display",
                "director",
                "producers",
                "studio_name",
                "show_type",
                "awards",
                "more_relevant_information",
                "season",
                "poster",
            ]
        )

        write.writerow(
            [
                sId,
                title,
                prov,
                title,
                title,
                title,
                title,
                title,
                summ,
                summ,
                "",
                director,
                "",
                "",
                "Serie",
                "",
                "",
                seas,
                "https://s3.amazonaws.com/[TU_BUCKET]/public/Antena3/Imagenes-Series/"
                + sId
                + "/"
                + sId
                + "_main.jpg",
            ]
        )

    bucket.upload_file(
        "/tmp/Genericos1.csv", "public/Antena3/csv/series/Genericos1.csv"
    )
