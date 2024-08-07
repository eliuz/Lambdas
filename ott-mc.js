var https = require('https');
var AWS = require('aws-sdk');
var s3 = new AWS.S3();
//var doc = require('dynamodb-doc');
//const dynamo = new AWS.DynamoDB();
var dynamo = new AWS.DynamoDB.DocumentClient();
let veryzon = require('verizon-uplynk-api');
let urn = new veryzon.API('[KEY]', '[API]');

exports.handler = (event, context, callback) => {
    
    dynamo.scan({
        TableName: '[TABLA_MOVIES]'
    }, function(error,data) {
        data.Items.forEach(function(pk) {
            dynamo.update({ 
            TableName: '[TABLA_MOVIES]',
            Key: {
                "externalId": pk.externalId
            },
        
            UpdateExpression: "set active = :valor",
            ExpressionAttributeValues: {
                //":valor": false
            }
            }, function(error, data) {
               // console.log(error, data);
            });
        })
    })

    var params = {
        Bucket: '[TU_BUCKET]',
        Prefix: 'public/MC/csv'
    }
    
    s3.listObjects(params, function(error, data) {
        data.Contents.forEach(function(informacion) {
           if(informacion.Key.indexOf('.csv')!=-1) {
               https.get('https://s3.amazonaws.com/[TU_BUCKET]/' + informacion.Key, function(response) {
                   let data='';
                   response.on('data', (chunk)=>{
                       data+=chunk;
                       
                   })
                   
                   response.on('end', ()=>{
                       
                      var separarFilas = data.split('\n');
                      separarFilas.forEach(function(filas, index) {
                          console.log('AQUI')
                         if(index>0 && index<separarFilas.length-1){
                            let playUrl;
                            var separarDatos = filas.split('|');
                            let url = correcion_Nombre(separarDatos[1].split(' ').join('_'));
                            let wallpaper1 = 'https://s3.amazonaws.com/[TU_BUCKET]/public/MC/Imagenes/' + separarDatos[0] + '/' + correcion_Nombre(separarDatos[1]) + '_highlight1.jpg';//'/' + separarDatos[0] + '_highlight1.jpeg';
                            let wallpaper2 = 'https://s3.amazonaws.com/[TU_BUCKET]/public/MC/Imagenes/' + separarDatos[0] + '/' + correcion_Nombre(separarDatos[1]) + '_highlight2.jpg';//'/' + separarDatos[0] + '_highlight2.jpeg';;
                            let poster = 'https://s3.amazonaws.com/[TU_BUCKET]/public/MC/Imagenes/' + separarDatos[0] + '/' + correcion_Nombre(separarDatos[1]) + '_main.jpg';//separarDatos[0] + '_main.jpeg';
                            let duracion = validarTiempo(separarDatos[13].split(''));
                            let segundos = (duracion[0]*36000) + (duracion[1]*3600) + (duracion[3]*600) + (duracion[4]*60);
                            let canal = (separarDatos[2] === 'MC') ? 'MC' : separarDatos[2];
                            let generos = [];
                            let clasificacion = [];
                            var participantes = [];
                            let separarGeneros = separarDatos[22].split(',');
                            let separarRatings = separarDatos[11].split(',');
                            let separarActores = separarDatos[16].split(',');
                            let separarEscritores = separarDatos[17].split(',');
                            let separarDirectores = separarDatos[18].split(',');
                            let separarProductores = separarDatos[19].split(',');
                           
                           
                            if(separarRatings.length == 1) {
                                clasificacion = separarRatings;
                            }
                        
                            for(var i=0; i<separarGeneros.length;i++) {
                                generos.push(separarGeneros[i]);
                            }
                        
                            for(var i=0; i<separarRatings; i++) {
                                clasificacion.push(separarRatings[i]);
                            }
                        
                            if((separarActores[0]+'')!='') {
                                for(var i=0; i<separarActores.length; i++) {
                                    participantes.push({
                                        "value": separarActores[i],
                                        "role": "ACTOR"
                                    });
                                }
                            }
                            
                            if((separarEscritores[0]+'')!='') {
                                for(var i=0; i<separarEscritores.length; i++) {
                                    participantes.push({
                                        "value": separarEscritores[i],
                                        "role": "WRITER"
                                    });
                                }
                            } 
                        
                        
                            if((separarDirectores[0]+'')!='') {
                                for(var i=0; i<separarDirectores.length; i++) {
                                    participantes.push({
                                        "value": separarDirectores[i],
                                        "role": "DIRECTOR"
                                    });
                                }
                            }
                            
                            if((separarProductores[0]+'')!='') {
                                for(var i=0; i<separarProductores.length; i++) {
                                    if(separarProductores[i]!='')
                                    participantes.push({
                                        "value": separarProductores[i],
                                        "role": "PRODUCER"
                                    });
                                }
                            }
                            
                            https.get(wallpaper1, (data)=>{
                                if(data.headers['content-type']!='image/jpeg')
                                {
                                    wallpaper1 = 'https://s3.amazonaws.com/[TU_BUCKET]/public/MC/Imagenes/' + separarDatos[0] + '/' + separarDatos[0] + '_highlight1.jpg';
                                    wallpaper2 = 'https://s3.amazonaws.com/[TU_BUCKET]/public/MC/Imagenes/' + separarDatos[0] + '/' + separarDatos[0] + '_highlight2.jpg';
                                    poster = 'https://s3.amazonaws.com/[TU_BUCKET]/public/MC/Imagenes/' + separarDatos[0] + '/' + separarDatos[0] + '_main.jpg';;
                                }
                            
                            urn.request('/api2/asset/get',{'external_id':separarDatos[0]}, (response)=>{
                                
                                if(response.error==1) console.log('NO ENCONTRADO: ' + separarDatos[0])
                                else 
                                        
                                       
                                         dynamo.put({
                                             
                                            TableName: "[TABLA_MOVIES]",
                                            Item:{
                                                "genres": generos,
                                                "participants": participantes,
                                                "externalId": separarDatos[0],
                                                "destinationUrl": [{
                                                    'HLS': 'https://[URL].com/api/v3/preplay/' + response.asset.id + '.json?rmt=fps&pp2ip=0',
                                                    'DASH': 'https://[URL].com/api/v3/preplay/' + response.asset.id + '.json?manifest=mpd&pp2ip=0'//'https://[URL].com/' + response.asset.id + '.mpd'
                                                }],
                                                "title": separarDatos[4],//datos.original_title,
                                                "alternativeTitle": separarDatos[6],
                                                "description": separarDatos[9],
                                                "ratings": clasificacion,
                                                "release": separarDatos[14],
                                                "duration": segundos,
                                                "isHD": response.asset.meta.isHD,
                                                "images": [{
                                                    "url": poster,
                                                    "width": 720,
                                                    "height": 1080,
                                                    "type": "POSTER"
                                                }, {
                                                    "url": wallpaper1,
                                                    "width": 1920,
                                                    "height": 1280,
                                                    "type": "THUMB"
                                                }, {
                                                    "url": wallpaper2,
                                                    "width": 1920,
                                                    "height": 1280,
                                                    "type": "THUMB"
                                                }],
                                                "channel": canal,
                                                "flights": [{
                                                    "start": separarDatos[23],
                                                    "end": separarDatos[24],
                                                }],
                                                "PrePlay": 'http://[URL].com/api/v3/preplay/' + response.asset.id + '.js?v=2&jsonp;=MyFunction',
                                                //"playURL": urlPrePlay.replace('"', ''), 
                                                "active": true
                                                
                                            }
                                          }, function(error, data) {
                                              //console.log(separarDatos[8], error, data);
                                              if(!data) /*console.log('Ingresado: ' + separarDatos[4]);
                                              else *//*console.log('No ingresado: ' + separarDatos[0], '\nGeneros: ' + generos, '\nParticipantes: ' + participantes,  '\nResponse: ' + response.asset.id, 
                                              '\nTitulo: ' + separarDatos[4], '\nTitulo alternativo: ' + separarDatos[6],  
                                              '\nDescripcion: ' + separarDatos[9], '\nRating: ' + clasificacion, '\nRelease: ' + separarDatos[14], '\nDuracion: ' + segundos,
                                              '\nisHD: ' + response.asset.meta.isHD, '\nStart: ' + separarDatos[23], '\nEnd: ' + separarDatos[24],  '\nWP1: ' + wallpaper1, '\nWP2: ' + wallpaper2,
                                              '\nChannel: ' + canal, '\nPoster: ' + poster);*/
                                              console.log('No ingresado: ' + separarDatos[1], canal, participantes)
                                          })
                            });
                           })
                         }    //Fin proceso de datos csv
                      });
                   });
               })
           }
        });
    });
};

function asignarGeneros(separarRatings) {
    
    var clasificacion = [];
    
    if(separarRatings.length == 1) {
        return separarRatings;
    }

    for(var i=0; i<separarRatings; i++) {
        clasificacion.push(separarRatings[i]);
    }
    return clasificacion;
}

function eliminarDiacriticos(texto) {
    return texto.normalize('NFD').replace(/[\u0300-\u0301]/g,"%CC%81");
}

function eliminaEnes(texto) {
    return texto.normalize('NFD').replace(/\u0303/g,"%CC%83"); //quitar la ñ
}

function eliminaDeresis(texto) {
    return texto.normalize('NFD').replace(/\u0308/g,"%CC%88"); //quitar la ñ
}

function correcion_Nombre(cadena) {
    let nombre = cadena.split(' ').join('_');
    nombre = eliminarDiacriticos(nombre.split(',').join('%2C'));
    nombre=nombre.split('¡').join('%C2%A1');
    nombre=nombre.split('¿').join('%C2%BF');
    nombre=nombre.split('?').join('%3F');
    nombre=nombre.split(':').join('%3A');
    nombre=eliminaDeresis(nombre);
    nombre=eliminaEnes(nombre);
    return nombre;
    
}

function validarTiempo(duracion) {
    var conteoValores=0;
    duracion.forEach(function(valores) {
    if(valores.indexOf(':')!=-1)
        conteoValores+=1;
    })
    
    if(conteoValores==2) return duracion[0]+duracion[1]+':'+'0'+duracion[3];
    else return duracion;
    
}