import {S3Client }  from "@aws-sdk/client-s3";
import { MediaConvertClient } from "@aws-sdk/client-mediaconvert";
import { CreateJobCommand } from "@aws-sdk/client-mediaconvert";
const s3 = new S3Client()
// Set the account end point.
const ENDPOINT = {
  endpoint: "[TU_MEDIACONVERT_ENDPOINT]",
};

// Set the MediaConvert Service Object
const emcClient = new MediaConvertClient(ENDPOINT);
export { emcClient };


export const handler = async (event)=> {
   

    
    const bucket = event.Records[0].s3.bucket.name;
    const key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
    var mediaparams ={
  "Queue": "[TU_MEDIACONVERT_QUEUE]",
  "UserMetadata": {},
  "Role": "[TU_MEDIACONVERT_ROL",
  "Settings": {
    "TimecodeConfig": {
      "Source": "ZEROBASED"
    },
    "OutputGroups": [
      {
        "CustomName": "ts-file",
        "Name": "File Group",
        "Outputs": [
          {
            "ContainerSettings": {
              "Container": "M2TS",
              "M2tsSettings": {
                "AudioBufferModel": "DVB",
                "PatInterval": 100,
                "VideoPid": 368,
                "PmtInterval": 100,
                "PmtPid": 480,
                "Bitrate": 2500000,
                "AudioPids": [
                  369
                ],
                "TransportStreamId": 50,
                "RateMode": "CBR",
                "AudioFramesPerPes": 6,
                "ProgramNumber": 1,
                "BufferModel": "MULTIPLEX",
                "DvbTeletextPid": 499
              }
            },
            "VideoDescription": {
              "Width": 720,
              "Height": 480,
              "CodecSettings": {
                "Codec": "H_264",
                "H264Settings": {
                  "InterlaceMode": "TOP_FIELD",
                  "ScanTypeConversionMode": "INTERLACED",
                  "ParNumerator": 10,
                  "NumberReferenceFrames": 4,
                  "FramerateDenominator": 1001,
                  "GopClosedCadence": 1,
                  "GopSize": 90,
                  "ParDenominator": 11,
                  "SpatialAdaptiveQuantization": "ENABLED",
                  "TemporalAdaptiveQuantization": "ENABLED",
                  "FlickerAdaptiveQuantization": "ENABLED",
                  "EntropyEncoding": "CABAC",
                  "Bitrate": 2200000,
                  "FramerateControl": "SPECIFIED",
                  "RateControlMode": "CBR",
                  "CodecProfile": "HIGH",
                  "FramerateNumerator": 30000,
                  "MinIInterval": 0,
                  "AdaptiveQuantization": "MEDIUM",
                  "CodecLevel": "LEVEL_3",
                  "FieldEncoding": "FORCE_FIELD",
                  "SceneChangeDetect": "ENABLED",
                  "GopSizeUnits": "FRAMES",
                  "ParControl": "SPECIFIED",
                  "NumberBFramesBetweenReferenceFrames": 2
                }
              },
              "ColorMetadata": "INSERT"
            },
            "AudioDescriptions": [
              {
                "AudioSourceName": "Audio Selector 1",
                "CodecSettings": {
                  "Codec": "MP2",
                  "Mp2Settings": {
                    "Bitrate": 192000,
                    "Channels": 2,
                    "SampleRate": 48000
                  }
                }
              }
            ],
            "Extension": "ts",
            //"NameModifier": "_basic2-250"
          }
        ],
        "OutputGroupSettings": {
          "Type": "FILE_GROUP_SETTINGS",
          "FileGroupSettings": {
            "Destination": "[TU_BUCKET_DESTINO]" # DONDE SE VA A GUARDAR EL ARCHIVO TRANSCODIFICADO
          }
        }
      }
    ],
    "Inputs": [
      {
        "AudioSelectors": {
          "Audio Selector 1": {
            "DefaultSelection": "DEFAULT"
          }
        },
        "VideoSelector": {
          "ColorSpace": "REC_601",
          "ColorSpaceUsage": "FORCE"
        },
        "TimecodeSource": "EMBEDDED",
        "FileInput": "[TU_BUCKET_INPUT]"+key     # DONDE SE VA A TOMAR EL ARCHIVO PARA TRANSCODIFICAR
      }
    ]
  },
  "AccelerationSettings": {
    "Mode": "DISABLED"
  },
  "StatusUpdateInterval": "SECONDS_60",
  "Priority": 0
}
    
try {
    const data = await emcClient.send(new CreateJobCommand(mediaparams));
        console.log("Job created!", data);
    return data;
  } catch (err) {
    console.log("Error", err);
  }
  

   
    
    
};