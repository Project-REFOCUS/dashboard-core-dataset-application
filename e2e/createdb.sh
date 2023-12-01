mysql -uroot -e "CREATE DATABASE changesets"
mysql -uroot "changesets" -e "
  CREATE TABLE project_refocus (
    id INTEGER UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    file_path VARCHAR(125) NOT NULL,
    applied TINYINT(1) NULL DEFAULT '1',
    environment VARCHAR(50) NOT NULL,
    reference_database VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP
  )
"
mysql -uroot -e "CREATE DATABASE project_refocus"
mysql_config_editor set --login-path=root --user root