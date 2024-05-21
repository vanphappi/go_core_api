print(
    "Start #################################################################"
  );
  
  db = db.getSiblingDB("api_prod_db");
  db.createUser({
    user: "core-api-admin-user",
    pwd: "admin-password",
    roles: [{ role: "readWrite", db: "core-api-db-name" }],
  });
  
  db.createCollection("users");
  
print("END ######################################");
  