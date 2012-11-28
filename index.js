/*
 * index.js : Crosstalk AWS EC2 API Worker
 *
 * (C) 2012 Crosstalk Systems Inc.
 */
"use strict";

var async = require( 'async' ),
    https = require( 'https' ),
    querystring = require( 'querystring' ),
    xml2js = require( 'xml2js' );

var API_ENDPOINT = "ec2.amazonaws.com",
    API_VERSION = "2012-10-01",
    HTTP_VERB = "GET",
    REQUEST_URI = "/";

var attachSignatureToRequest = function attachSignatureToRequest ( dataBag, callback ) {

  dataBag.query[ 'Signature' ] = 
     decodeURIComponent( dataBag.requestSignature.signature );
  dataBag.query[ 'SignatureMethod' ] = dataBag.requestSignature.signatureMethod;
  dataBag.query[ 'SignatureVersion' ] = 
     dataBag.requestSignature.signatureVersion;

  dataBag.queryString = querystring.stringify( dataBag.query );

  return callback( null, dataBag );

}; // attachSignatureToRequest

var constructQuery = function constructQuery( action, awsAccessKeyId ) {

  return {
    //
    // common query params
    //
    Action : action,
    Version : API_VERSION,
    AWSAccessKeyId : awsAccessKeyId,
    Timestamp : ( new Date() ).toISOString()
  };

}; // constructQuery

var describeInstances = function describeInstances ( params, callback ) {

  if ( ! callback ) { return; } // nothing to do

  //
  // required params
  //
  var awsAccessKeyId = params.awsAccessKeyId,
      secretAccessKey = params.secretAccessKey;

  if ( ! awsAccessKeyId ) return callback( { message : "missing awsAccessKeyId" } );
  if ( ! secretAccessKey ) return callback( { message : "missing secretAccessKey" } );

  //
  // optional params
  //
  var filters = params.filters,
      instanceIds = params.instanceIds,
      securityToken = params.securityToken;

  var query = constructQuery( "DescribeInstances", awsAccessKeyId );

  var index, secondaryIndex;

  //
  // action required params
  //
  // None

  //
  // action optional params
  //
  if ( filters ) {

    index = 1;

    Object.keys( filters ).forEach( function ( key ) {

      query[ 'Filter.' + index + '.Name' ] = key;

      var value = filters[ key ];
      secondaryIndex = 1;

      if ( Array.isArray( value ) && value.length > 0 ) {

        value.forEach( function ( val ) {

          query[ 'Filter.' + index + '.Value.' + secondaryIndex ] = val;
          secondaryIndex++;

        }); // value.forEach

      } else {
        query[ 'Filter.' + index + '.Value.' + secondaryIndex ] = value;
      }

      index++;

    }); // Object.keys( filters ).forEach

  } // if ( filters )
  if ( instanceIds ) {

    index = 1;

    instanceIds.forEach( function ( instance ) {

      query[ "InstanceId." + index ] = instance;

      index ++;

    }); // instanceIds.forEach

  } // if ( instanceIds ) 
  if ( securityToken ) query[ 'SecurityToken' ] = securityToken;

  return executeAction( awsAccessKeyId, query, secretAccessKey, callback );

}; // describeInstances

var executeAction = function executeAction ( awsAccessKeyId, query, 
     secretAccessKey, callback ) {

  var queryString = querystring.stringify( query );

  async.waterfall([

    function ( _callback ) {
      
      return _callback( null, {
        awsAccessKeyId : awsAccessKeyId,
        query : query,
        queryString : queryString,
        secretAccessKey : secretAccessKey
      });

    }, // bootstrap dataBag

    getRequestSignature,

    attachSignatureToRequest,

    makeRequest,

    parseResponse

  ], function ( error, result ) {

    if ( error ) { return callback( error ); }

    return callback( null, result );

  }); // async.waterfall

}; // executeAction

var getRequestSignature = function getRequestSignature ( dataBag, callback ) {

  crosstalk.emit( '~crosstalk.api.aws.signature.version2', {
    awsAccessKeyId : dataBag.awsAccessKeyId,
    host : API_ENDPOINT,
    queryString : dataBag.queryString,
    secretAccessKey : dataBag.secretAccessKey
  }, '~crosstalk', function ( error, response ) {

    if ( error ) { return callback( error ); }

    dataBag.requestSignature = response;
    return callback( null, dataBag );

  }); // crosstalk.emit ~crosstalk.api.aws.ec2.requestSignature

}; // getRequestSignature

var makeRequest = function makeRequest ( dataBag, callback ) {

  var queryString = dataBag.queryString;
  
  var requestOptions = {
    host : API_ENDPOINT,
    method : HTTP_VERB,
    path : REQUEST_URI + "?" + queryString
  };

  var req = https.request( requestOptions );

  req.on( 'response', function ( response ) {

    var body = "";

    response.setEncoding( 'utf8' );
    response.on( 'data', function ( chunk ) {
      body += chunk;
    });

    response.on( 'end', function () {

      dataBag.response = response;
      dataBag.responseBody = body;

      return callback( null, dataBag );

    }); // response.on 'end'

  }); // req.on 'response'

  req.on( 'error', function ( error ) {
    return callback( error );
  });

  req.end();

}; // makeRequest

var normalizeJson = function normalizeJson ( obj ) {

  Object.keys( obj ).forEach( function ( key ) {

    if ( typeof( obj[ key ] ) === "object" ) {

      if ( Object.keys( obj[ key ] ).length == 0 ) {
        delete obj[ key ];
      } else {
        obj[ key ] = normalizeJson( obj[ key ] );
      }

    } // if ( typeof( obj[ key ] ) === "object" )
    else if ( obj[ key ] == "true" ) {
      obj[ key ] = true;
    } else if ( obj[ key ] == "false" ) {
      obj[ key ] = false;
    }

  }); // Object.keys( obj ).forEach

  return obj;

}; // normalizeJson

var parseResponse = function parseResponse ( dataBag, callback ) {

  var body = dataBag.responseBody,
      response = dataBag.response;

  if ( response.statusCode != 200 ) {
    return callback( body );
  }

  var parser = new xml2js.Parser();

  parser.parseString( body, function ( error, result ) {

    if ( error ) { return callback( error ); }

    delete result[ '@' ]; // get rid of xml artifact

    // remove parsed empty objects {} and convert "<boolean>" to <boolean>
    result = normalizeJson( result );

    return callback( null, result );

  }); // parser.parseString

}; // parseResponse

var runInstances = function runInstances ( params, callback ) {

  callback = callback || function () {}; // req-reply pattern is optional

  //
  // required params
  //
  var awsAccessKeyId = params.awsAccessKeyId,
      imageId = params.imageId,
      secretAccessKey = params.secretAccessKey;

  if ( ! awsAccessKeyId ) return callback( { message : "missing awsAccessKeyId" } );
  if ( ! imageId ) return callback( { message : "missing imageId" } );
  if ( ! secretAccessKey ) return callback( { message : "missing secretAccessKey" } );

  //
  // optional params
  //
  var availabilityZone = params.availabilityZone 
        || params.placementAvailabilityZone,
      blockDeviceMappings = params.blockDeviceMappings,
      clientToken = params.clientToken,
      disableApiTermination = params.disableApiTermination,
      ebsOptimized = params.ebsOptimized,
      groupName = params.groupName || params.placementGroupName,
      kernelId = params.kernelId,
      keyName = params.keyName,
      iamInstanceProfileArn = params.iamInstanceProfileArn,
      iamInstanceProfileName = params.iamInstanceProfileName,
      instanceInitiatedShutdownBehavior = 
         params.instanceInitiatedShutdownBehavior,
      instanceType = params.instanceType,
      maxCount = params.maxCount || 1,
      minCount = params.minCount || 1,
      monitoringEnabled = params.monitoringEnabled,
      networkInterfaces = params.networkInterfaces,
      privateIpAddress = params.privateIpAddress,
      ramdiskId = params.ramdiskId,
      securityGroups = params.securityGroups,
      securityGroupIds = params.securityGroupIds,
      securityToken = params.securityToken,
      subnetId = params.subnetId,
      tenancy = params.tenancy || params.placementTenancy,
      userData = params.userData;

  var query = constructQuery( "RunInstances", awsAccessKeyId );

  var index, secondaryIndex;

  //
  // action required params
  //
  query[ 'ImageId' ] = imageId;
  query[ 'MaxCount' ] = maxCount;
  query[ 'MinCount' ] = minCount;

  //
  // action optional params
  //
  if ( availabilityZone ) query[ 'Placement.AvailabilityZone' ] = availabilityZone;
  if ( blockDeviceMappings ) {
    
    index = 1;
    blockDeviceMappings.forEach( function ( blDev ) {

      var blockDevice = 'BlockDeviceMapping.' + index + '.';

      if ( blDev.deviceName ) {
        query[ blockDevice + 'DeviceName' ] = blDev.deviceName;
      }
      if ( blDev.noDevice ) query[ blockDevice + 'NoDevice' ] = "";
      if ( blDev.virtualName ) {
        query[ blockDevice + 'VirtualName' ] = blDev.virtualName;
      }
      if ( blDev.ebs ) {
        if ( blDev.ebs.snapshotId ) {
          query[ blockDevice + 'Ebs.SnapshotId' ] = blDev.ebs.snapshotId;
        }
        if ( blDev.ebs.volumeSize ) {
          query[ blockDevice + 'Ebs.VolumeSize' ] = blDev.ebs.volumeSize;
        }
        // could be false
        if ( typeof( blDev.ebs.deleteOnTermination ) != 'undefined' ) { 
          query[ blockDevice + 'Ebs.DeleteOnTermination' ] = 
             blDev.ebs.deleteOnTermination;
        }
        if ( blDev.ebs.volumeType ) {
          query[ blockDevice + 'Ebs.VolumeType' ] = blDev.ebs.volumeType;
        }
        if ( blDev.ebs.iops ) {
          query[ blockDevice + 'Ebs.Iops' ] = blDev.ebs.iops;
        }
      }

      index++;

    }); // blockDeviceMappings.forEach 

  } // if ( blockDeviceMappings )
  if ( clientToken ) query[ 'ClientToken' ] = clientToken;
  if ( disableApiTermination ) query[ 'DisableApiTermination' ] = disableApiTermination;
  if ( ebsOptimized ) query[ 'EbsOptimized' ] = ebsOptimized;
  if ( groupName ) query[ 'Placement.GroupName' ] = groupName;
  if ( kernelId ) query[ 'KernelId' ] = kernelId;
  if ( keyName ) query[ 'KeyName' ] = keyName;
  if ( iamInstanceProfileArn ) query[ 'IamInstanceProfile.Arn' ] = iamInstanceProfileArn;
  if ( iamInstanceProfileName ) query[ 'IamInstanceProfile.Name' ] = iamInstanceProfileName;
  if ( instanceInitiatedShutdownBehavior ) query[ 'InstanceInitiatedShutdownBehavior' ] = instanceInitiatedShutdownBehavior;
  if ( instanceType ) query[ 'InstanceType' ] = instanceType;
  if ( monitoringEnabled ) query[ 'MonitoringEnabled' ] = monitoringEnabled;
  if ( networkInterfaces ) {

    index = 1;

    networkInterfaces.forEach( function ( ni ) {

      var netInt = 'NetworkInterface.' + index + '.';

      if ( ni.networkInterfaceId ) {
        query[ netInt + 'NetworkInterfaceId' ] = ni.networkInterfaceId;
      }
      if ( ni.deviceIndex ) query[ netInt + 'DeviceIndex' ] = ni.deviceIndex;
      if ( ni.subnetId ) query[ netInt + 'SubnetId' ] = ni.subnetId;
      if ( ni.description ) query[ netInt + 'Description' ] = ni.description;
      if ( ni.privateIpAddress ) {
        query[ netInt + 'PrivateIpAddress' ] = ni.privateIpAddress;
      }
      if ( ni.privateIpAddresses ) {

        secondaryIndex = 1;

        ni.privateIpAddresses.forEach( function ( privIpAdd ) {

          if ( privIpAdd.privateIpAddress ) {
            query[ netInt + 'PrivateIpAddresses.' + secondaryIndex + 
               '.PrivateIpAddress' ] = privIpAdd.privateIpAddress;
          }
          if ( privIpAdd.primary ) {
            query[ netInt + 'PrivateIpAddresses.' + secondaryIndex +
               '.Primary' ] = privIpAdd.primary;
          }

          secondaryIndex++;

        }); // ni.privateIpAddresses.forEach

      } // if ( ni.privateIpAddresses )
      if ( ni.secondaryPrivateIpAddressCount ) {
        query[ netInt + 'SecondaryPrivateIpAddressCount' ] =
           ni.secondaryPrivateIpAddressCount;
      }
      if ( ni.securityGroupIds ) {

        secondaryIndex = 1;

        ni.securityGroupIds.forEach( function ( secGrp ) {

          query[ netInt + 'SecurityGroupId.' + secondaryIndex ] = secGrp;
          secondaryIndex++;

        }); // ni.securityGroupIds.forEach

      } // if ( ni.securityGroupIds )
      if ( ni.deleteOnTermination ) {
        query[ netInt + 'DeleteOnTermination' ] = ni.deleteOnTermination;
      }

      index++;

    }); // networkInterfaces.forEach

  } // if ( networkInterfaces )
  if ( privateIpAddress ) query[ 'PrivateIpAddress' ] = privateIpAddress;
  if ( ramdiskId ) query[ 'RamdiskId' ] = ramdiskId;
  if ( securityGroups ) {

    index = 1;

    securityGroups.forEach( function ( securityGroup ) {

      query[ 'SecurityGroup.' + index ] = securityGroup;
      index++;

    }); // securityGroups.forEach

  } // if ( securityGroups )
  if ( securityGroupIds ) {

    index = 1;

    securityGroupIds.forEach( function ( securityGroupId ) {

      query[ 'SecurityGroupId.' + index ] = securityGroupId;
      index++;

    }); // securityGroupIds.forEach

  } // if ( securityGroupIds )
  if ( securityToken ) query[ 'SecurityToken' ] = securityToken;
  if ( subnetId ) query[ 'SubnetId' ] = subnetId;
  if ( tenancy ) query[ 'Placement.Tenancy' ] = tenancy;
  if ( userData ) query[ 'UserData' ] = userData;

  return executeAction( awsAccessKeyId, query, secretAccessKey, callback );

}; // runInstances

var startInstances = function startInstances ( params, callback ) {

  callback = callback || function () {}; // req-reply pattern is optional

  //
  // required params
  //
  var awsAccessKeyId = params.awsAccessKeyId,
      instanceIds = params.instanceIds,
      secretAccessKey = params.secretAccessKey;

  if ( ! awsAccessKeyId ) return callback( { message : "missing awsAccessKeyId" } );
  if ( ! instanceIds ) return callback( { message : "missing instanceIds" } );
  if ( ! Array.isArray( instanceIds ) ) return callback( { message : "instanceIds is not an array" } );
  if ( ! secretAccessKey ) return callback( { message : "missing secretAccessKey" } );

  //
  // optional params
  //
  var securityToken = params.securityToken;

  var query = constructQuery( "StartInstances", awsAccessKeyId );

  //
  // action required params
  //
  var index;

  index = 1;
  instanceIds.forEach( function ( instance ) {

    query[ "InstanceId." + index ] = instance;

    index ++;

  }); // instanceIds.forEach

  //
  // action optional params
  //
  if ( securityToken ) query[ 'SecurityToken' ] = securityToken;

  return executeAction( awsAccessKeyId, query, secretAccessKey, callback );

}; // startInstances

var stopInstances = function stopInstances ( params, callback ) {

  callback = callback || function () {}; // req-reply pattern is optional

  //
  // required params
  //
  var awsAccessKeyId = params.awsAccessKeyId,
      instanceIds = params.instanceIds,
      secretAccessKey = params.secretAccessKey;

  if ( ! awsAccessKeyId ) return callback( { message : "missing awsAccessKeyId" } );
  if ( ! instanceIds ) return callback( { message : "missing instanceIds" } );
  if ( ! Array.isArray( instanceIds ) ) return callback( { message : "instanceIds is not an array" } );
  if ( ! secretAccessKey ) return callback( { message : "missing secretAccessKey" } );

  //
  // optional params
  //
  var force = params.force,
      securityToken = params.securityToken;

  var query = constructQuery( "StopInstances", awsAccessKeyId );

  //
  // action required params
  //
  var index;

  index = 1;
  instanceIds.forEach( function ( instance ) {

    query[ "InstanceId." + index ] = instance;

    index ++;

  }); // instanceIds.forEach

  //
  // action optional params
  //
  if ( force ) query[ 'Force' ] = force;
  if ( securityToken ) query[ 'SecurityToken' ] = securityToken;

  return executeAction( awsAccessKeyId, query, secretAccessKey, callback );

}; // stopInstances

var terminateInstances = function terminateInstances ( params, callback ) {

  callback = callback || function () {}; // req-reply pattern is optional

  //
  // required params
  //
  var awsAccessKeyId = params.awsAccessKeyId,
      instanceIds = params.instanceIds,
      secretAccessKey = params.secretAccessKey;

  if ( ! awsAccessKeyId ) return callback( { message : "missing awsAccessKeyId" } );
  if ( ! instanceIds ) return callback( { message : "missing instanceIds" } );
  if ( ! Array.isArray( instanceIds ) ) return callback( { message : "instanceIds is not an array" } );
  if ( ! secretAccessKey ) return callback( { message : "missing secretAccessKey" } );

  //
  // optional params
  //
  var securityToken = params.securityToken;

  var query = constructQuery( "TerminateInstances", awsAccessKeyId );

  var index;

  //
  // action required params
  //
  index = 1;
  instanceIds.forEach( function ( instance ) {

    query[ "InstanceId." + index ] = instance;

    index ++;

  }); // instanceIds.forEach

  //
  // action optional params
  //
  if ( securityToken ) query[ 'SecurityToken' ] = securityToken;

  return executeAction( awsAccessKeyId, query, secretAccessKey, callback );

}; // terminateInstances

crosstalk.on( 'api.aws.ec2.describeInstances', 'public', describeInstances );
crosstalk.on( 'api.aws.ec2.runInstances', 'public', runInstances );
crosstalk.on( 'api.aws.ec2.startInstances', 'public', startInstances );
crosstalk.on( 'api.aws.ec2.stopInstances', 'public', stopInstances );
crosstalk.on( 'api.aws.ec2.terminateInstances', 'public', terminateInstances );