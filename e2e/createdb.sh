mysql -e "CREATE DATABASE changesets"
mysql "changesets" -e "
  CREATE TABLE project_refocus (
    id INTEGER UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    file_path VARCHAR(125) NOT NULL,
    applied TINYINT(1) NULL DEFAULT '1',
    environment VARCHAR(50) NOT NULL,
    reference_database VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP
  )
"
mysql -e "CREATE DATABASE project_refocus"