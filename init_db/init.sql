CREATE SCHEMA de;
CREATE USER de_dwh PASSWORD 'de_dwh_password';
GRANT ALL ON SCHEMA de TO de_dwh;
GRANT ALL ON SCHEMA public TO de_dwh;
