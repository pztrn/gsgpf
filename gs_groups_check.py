#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

# GNU Social groups checker.
# Checks for groups profiles in "profile" table, and add missing,
# if applicable.
# Copyright (c) 2013, Stanislav N. aka pztrn (pztrn on pztrn dot name)

# Dependencies:
#   * python2.7-mysqldb

import os
import sys
import json
try:
    import MySQLdb
except:
    print "MySQL bindings for python not installed."
    exit(10)

class Check_Groups():
    def __init__(self):
        cfg = json.loads(open("gs_groups_check.config.json", "r").read())
        # Groups that will be added.
        self.groups_to_add = []
        # Groups that have profiles, but with wrong profile_id
        self.groups_to_modify = []
        
        print "Connecting to MySQL..."
        try:
            self.db_con = MySQLdb.connect(cfg["host"], cfg["user"], cfg["pass"], cfg["dbname"])
            print "Connection established."
            self.db = self.db_con.cursor(MySQLdb.cursors.DictCursor)
            #self.db_con.paramstyle("named")
            self.db.execute("SET NAMES `utf8`")
        except Exception as e:
            print "Failed to connect to MySQL: " + e[1]
            exit(1)
            
        self.get_groups_data()
        self.get_profiles_data()
        self.get_subscribed_groups()
        self.check_groups_presence()
        self.add_groups_profiles()
        self.modify_groups()
        self.finish()
        
    def get_groups_data(self):
        print "Obtaining groups information..."
        self.db.execute("SELECT * FROM user_group")
        self.groups_data = self.db.fetchall()
        print "Found {0} groups".format(self.db.rowcount)
    
    def get_profiles_data(self):
        print "Obtaining profiles information..."
        self.db.execute("SELECT * FROM profile")
        self.profiles_data = self.db.fetchall()
        print "Found {0} profiles".format(self.db.rowcount)
        
    def get_subscribed_groups(self):
        print "Obtaining information about groups subscribing..."
        self.db.execute("SELECT group_id FROM group_member")
        subscribed_groups = self.db.fetchall()
        print "Found {0} subscribe records".format(self.db.rowcount)
        # We just don't want our list to re like [(0,), (1,)].
        self.subscribed_groups = []
        for item in subscribed_groups:
            self.subscribed_groups.append(item["group_id"])
        
    def check_groups_presence(self):
        print "Checking groups..."
        for item in self.groups_data:
            if item["id"] in self.subscribed_groups and item["profile_id"] == 0:
                found, correct, profile_id = self.check_profiles_for_group(item["nickname"], item["profile_id"])
                if not found:
                    print "{0} in subscribed, but profile data not found. It's profile data will be added".format(item["nickname"])
                    self.groups_to_add.append(item)
                elif found and not correct:
                    print "{0} found in profiles, but group item contains wrong profile_id. This will be corrected".format(item["nickname"])
                    self.groups_to_modify.append({"group_id" :item["id"], "profile_id" :profile_id})
                    
    def check_profiles_for_group(self, group_name, group_id):
        #print group_name, group_id
        found = False
        correct = False
        profile_id = 0
        for item in self.profiles_data:
            if item["nickname"].lower() == group_name.lower():
                found = True
                profile_id = item["id"]
                
            if not group_id == 0 and item["id"] == group_id:
                correct = True
                
            if found:
                break
        
        return (found, correct, profile_id)
                
                    
    def add_groups_profiles(self):
        print "About to add {0} groups profiles...".format(len(self.groups_to_add))
        for item in self.groups_to_add:
            try:
                self.db.execute("INSERT INTO profile (nickname, fullname, profileurl, location, bio, created, modified) VALUES (%(nickname)s, %(fullname)s, %(uri)s, %(location)s, %(description)s, %(created)s, %(modified)s)", item)
            except MySQLdb.Error, e:
                self.db_con.rollback()
                self.db_con.close()
                print "Error while executing query: " + e[1]
                exit(2)
            
        self.db_con.commit()
        
    def modify_groups(self):
        print "About to modify {0} groups profiles with incorrect profile_id...".format(len(self.groups_to_modify))
        for item in self.groups_to_modify:
            try:
                self.db.execute("UPDATE user_group SET profile_id = %(profile_id)s WHERE id = %(group_id)s", item)
            except MySQLdb.Error, e:
                self.db_con.rollback()
                self.db_con.close()
                print "Error while executing query: " + e[1]
                exit(2)

        self.db_con.commit()
        
    def finish(self):
        print "Completed."
        self.db.close()
        exit()

if __name__ == "__main__":
    Check_Groups()
