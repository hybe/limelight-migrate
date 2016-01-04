#! /usr/bin/env node
console.log('limelight-migrate')
var AWS = require('aws-sdk');
var request = require('request')
var _ = require('lodash')
var fs = require('fs')
var q = require('q')
var zlib = require('zlib');
var async = require('async')

var BASE_PATH = './content'
var MANIFEST_FILE = 'manifest.json'
var manifest = []
var content = JSON.parse(fs.readFileSync('content.json'));

if(!fs.existsSync(BASE_PATH))
	fs.mkdirSync(BASE_PATH)

console.log('Loaded', content.length, 'items')

function errorLocation(id, message) {
	console.log('Reporting error', message)
	fs.writeFileSync(BASE_PATH + '/' + id + '/error.txt', message)
}

function updateMaster(item) {
	var manifest = []
	if(fs.existsSync(MANIFEST_FILE))
		manifest = JSON.parse(fs.readFileSync(MANIFEST_FILE));
	manifest.push(item)
	fs.writeFileSync(MANIFEST_FILE, JSON.stringify(manifest))
}


function downloadFile(url, folder, location) {
	var deferred = q.defer()
	request
				.get(url)
				.on('response', function(response) {
					console.log('Downloading',response.headers['content-type'])
				})
				.on('error', function(err) {
					console.log('Error fetching', folder, err)
					errorLocation(location, err)
					deferred.reject(err)
				})
				.on('data', function(data) {
					//console.log(data.length,'bytes')
				})
				.on('end', function() {
					deferred.resolve(folder + '/' + location)
				})
				.pipe(fs.createWriteStream(folder + '/' + location))		

	return deferred.promise
}

function download(item, ready) {
	var folder = BASE_PATH + '/' + item.media_id;
	if(item.download_size === 0 || (fs.existsSync(folder) && fs.existsSync(folder + '/' + MANIFEST_FILE))) {
			console.log('Skipping', item.title)
			ready(null)
			return;
	} else {
		if(!fs.existsSync(folder))
			fs.mkdirSync(folder)
		console.log('Downloading', item.title, 'ID', item.media_id)
		var promises = []
		var fname = item.media_id + '_' + item.original_filename + '.mp4'
		promises.push(downloadFile(item.thumbnail1, folder, item.media_id + '_small.jpg'))
		promises.push(downloadFile(item.thumbnail2, folder, item.media_id + '_large.jpg'))
		promises.push(downloadFile(item.download_url, folder, fname))

		q.all(promises).then(function(res) {
			console.log('Downloads complete:', res)
			
			var info = {
				mediaID: item.media_id,
				date: Math.floor(parseFloat(item.date)),
				title: item.title,
				thumbnails: [item.media_id + '_small.jpg', item.media_id + '_large.jpg'],
				tags: item.tags,
				size: item.download_size,
				description: item.description,
				media: fname
			}
			console.log('Media file sizes match, writing to S3')
			fs.writeFileSync(folder + '/' + MANIFEST_FILE + '.upload', JSON.stringify(info))
			
			upload(info).then(function(res) {
				console.log('Completed upload, writing manifest', res)
				fs.writeFileSync(folder + '/' + MANIFEST_FILE, JSON.stringify(info))
				fs.unlink(folder + '/' + MANIFEST_FILE + '.upload')
				updateMaster(info)
				ready(null, info.mediaID)
			}, function(err) {
				errorLocation(item.media_id, 'Error while uploading:', err)
				ready(err)
			})
			
		}, function(err) {
			errorLocation(item.media_id, 'Fatal error ' + JSON.stringify(err))
			ready(err)
		})
	}
}

function s3Upload(localFile, remoteFile) {
	var deferred = q.defer()
	var body = fs.createReadStream(localFile)
	var s3Object = new AWS.S3({params: {Bucket: 'hybe-backup', Key: 'HYBE-Stories/' + remoteFile}});
	s3Object.upload({Body: body})  //.on('httpUploadProgress', function(evt) { })
		.send(function(err, data) { 
			if(err) {
				deferred.reject(err)
			} else {
				deferred.resolve(data.Location)
			}
		});

	return deferred.promise
}

function upload(item) {
	var folder = BASE_PATH + '/' + item.mediaID;
	return q.all([
		s3Upload(folder + '/' + item.mediaID + '_small.jpg', item.mediaID + '/' + item.mediaID + '_small.jpg'), 
		s3Upload(folder + '/' + item.mediaID + '_large.jpg', item.mediaID + '/' + item.mediaID + '_large.jpg'), 
		s3Upload(folder + '/' + item.media, item.mediaID + '/' + item.media), 
		s3Upload(folder + '/' + MANIFEST_FILE + '.upload', item.mediaID + '/' + MANIFEST_FILE)]
	)
}

async.eachLimit(content, 3, download, function(err) {
	 if( err ) {
      // One of the iterations produced an error.
      // All processing will now stop.
      console.log('A file failed to process:', err);
    } else {
      console.log('All files have been processed successfully');
    }
})

