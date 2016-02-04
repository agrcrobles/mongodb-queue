/**
 *
 * mongodb-queue.js - Use your existing MongoDB as a local queue.
 *
 * Copyright (c) 2014 Andrew Chilton
 * - http://chilts.org/
 * - andychilton@gmail.com
 *
 * License: http://chilts.mit-license.org/2014/
 *
 **/
"use strict";

const crypto = require("crypto");

// some helper functions
function id() {
	return crypto.randomBytes(16).toString("hex");
}

function now() {
	return new Date().toISOString();
}

function nowPlusSecs(secs) {
	return (new Date(Date.now() + secs * 1000)).toISOString();
}

module.exports = function (mongoDbClient, name, opts) {
	return new Queue(mongoDbClient, name, opts);
};

// the Queue object itself
function Queue(mongoDbClient, name, opts) {
	if (!mongoDbClient) {
		throw new Error("mongodb-queue: provide a mongodb.MongoClient");
	}

	if (!name) {
		throw new Error("mongodb-queue: provide a queue name");
	}

	opts = opts || {};

	this.name = name;
	this.col = mongoDbClient.collection(name);
	this.visibility = opts.visibility || 30;
	this.delay = opts.delay || 0;

	if (opts.deadQueue) {
		this.deadQueue = opts.deadQueue;
		this.maxRetries = opts.maxRetries || 5;
	}
}

Queue.prototype.createIndexes = function (callback) {
	const self = this;

	self.col.createIndex({ deleted: 1, visible: 1, _id: 1 }, (err, indexname) => {
		if (err) {
			return callback(err);
		}

		self.col.createIndex({ ack: 1 }, { unique: true, sparse: true }, (innerErr) => {
			if (innerErr) {
				return callback(innerErr);
			}

			callback(null, indexname);
		});
	});
};

Queue.prototype.add = function (payload, opts, callback) {
	const self = this;

	if (!callback) {
		callback = opts;
		opts = {};
	}

	const delay = opts.delay || self.delay;
	const msg = {
		visible: delay ? nowPlusSecs(delay) : now(),
		payload: payload,
	};

	self.col.insertOne(msg, (err, results) => {
		if (err) {
			return callback(err);
		}

		callback(null, (results.ops || results)[0]._id.toHexString());
	});
};

Queue.prototype.get = function (opts, callback) {
	const self = this;

	if (!callback) {
		callback = opts;
		opts = {};
	}

	const user = opts.user;
	const visibility = opts.visibility || self.visibility;
	const query = {
		visible: { $lt: now() },
		deleted: { $exists: false },
	};
	const sort = {
		_id: 1
	};
	const update = {
		$inc: { tries: 1 },
		$set: {
			ack: id(),
			visible: nowPlusSecs(visibility),
			firstClaimed: now()
		}
	};

	if (user) {
		update.user = user;
	}

	self.col.findOneAndUpdate(query, update, { sort: sort, returnOriginal: false }, (err, result) => {
		if (err) {
			return callback(err);
		}

		let msg = result.value;

		if (!msg) {
			return callback();
		}

		msg.id = msg._id.toHexString();
		delete msg._id;

		// if we have a deadQueue, then check the tries, else don't
		if (self.deadQueue) {
			// check the tries
			if (msg.tries > self.maxRetries) {
				// So:
				// 1) add this message to the deadQueue
				// 2) ack this message from the regular queue
				// 3) call ourself to return a new message (if exists)
				self.deadQueue.add(msg, (addErr) => {
					if (addErr) {
						return callback(addErr);
					}

					self.ack(msg.ack, (ackErr) => {
						if (ackErr) {
							return callback(ackErr);
						}

						self.get(callback);
					});
				});
				return;
			}
		}
		callback(null, msg);
	});
};

Queue.prototype.ping = function (ack, opts, callback) {
	const self = this;

	if (!callback) {
		callback = opts;
		opts = {};
	}

	const visibility = opts.visibility || self.visibility;
	const query = {
		ack: ack,
		visible: { $gt: now() },
		deleted: { $exists: false },
	};
	const update = {
		$set: {
			visible: nowPlusSecs(visibility)
		}
	};

	self.col.findOneAndUpdate(query, update, { returnOriginal: false }, (err, msg) => {
		if (err) {
			return callback(err);
		}

		if (!msg.value) {
			return callback(new Error("Queue.ping(): Unidentified ack  : " + ack));
		}

		callback(null, msg.value._id.toHexString());
	});
};

Queue.prototype.ack = function (ack, callback) {
	const self = this;

	const query = {
		ack: ack,
		visible: { $gt: now() },
		deleted: { $exists: false },
	};
	const update = {
		$set: {
			deleted: now(),
		}
	};

	self.col.findOneAndUpdate(query, update, { returnOriginal: false }, (err, msg) => {
		if (err) {
			return callback(err);
		}

		if (!msg.value) {
			return callback(new Error("Queue.ack(): Unidentified ack : " + ack));
		}

		callback(null, msg.value._id.toHexString());
	});
};

Queue.prototype.clean = function (callback) {
	const self = this;

	const query = {
		deleted: { $exists: true },
	};

	self.col.deleteMany(query, callback);
};

Queue.prototype.total = function (callback) {
	const self = this;

	self.col.count((err, count) => {
		if (err) {
			return callback(err);
		}

		callback(null, count);
	});
};

Queue.prototype.size = function (callback) {
	const self = this;

	const query = {
		visible: { $lt: now() },
		deleted: { $exists: false },
	};

	self.col.count(query, (err, count) => {
		if (err) {
			return callback(err);
		}

		callback(null, count);
	});
};

Queue.prototype.inFlight = function (callback) {
	const self = this;

	const query = {
		visible: { $gt: now() },
		ack: { $exists: true },
		deleted: { $exists: false },
	};

	self.col.count(query, (err, count) => {
		if (err) {
			return callback(err);
		}

		callback(null, count);
	});
};

Queue.prototype.done = function (callback) {
	const self = this;

	const query = {
		deleted: { $exists: true },
	};

	self.col.count(query, (err, count) => {
		if (err) {
			return callback(err);
		}

		callback(null, count);
	});
};
