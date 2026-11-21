#!/bin/sh
set -e

mongosh "mongodb://${MONGO_ROOT_USER}:${MONGO_ROOT_PWD}@${MONGO_HOST}/admin" --quiet --eval '
  const dbName = process.env.AUTH2_MONGO_DB;
  const user = process.env.AUTH2_MONGO_USER;
  const pwd = process.env.AUTH2_MONGO_PWD;
  try {
    db.getSiblingDB(dbName).createUser({
      user: user,
      pwd: pwd,
      roles: [{ role: "readWrite", db: dbName }],
    });
    print("auth2 mongo user created");
  } catch (e) {
    if (/already exists/i.test(e.message)) {
      print("auth2 mongo user already exists");
    } else {
      throw e;
    }
  }
'
