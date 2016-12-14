#!/usr/bin/lua5.1
--require"luasql.sqlite3"
--wget software77.net/geo-ip/?DL=1 -O /path/IpToCountry.csv.gz
tdb = {
  ["ip2country_db_file"]="/tmp/IpToCountry.csv"
	["mysql"] = {
		use = true,
		user = "",
		passwd = "",
		serv_addr = "localhost",
		port = "3306",
		db = "ip2country",
	},
}
if tdb["mysql"].use then
	require("luasql.mysql")
	env = assert(luasql.mysql())
end
function connect_to_mysql()
        if not con or not con:execute("USE "..tdb["mysql"].db) then
                con = assert(env:connect(tdb["mysql"].db,tdb["mysql"].user,tdb["mysql"].passwd,tdb["mysql"].serv_addr,tdb["mysql"].port))
                if con then
                        con:execute("SET NAMES utf8")                          
                        return true
                end
        else
                return true
        end
end


function mysql_table_create()
  assert(con:execute("DROP TABLE IF EXISTS ip2country"))
  assert(con:execute(
  [[CREATE TABLE ip2country(
  	`start_ip` int(11) unsigned NOT NULL,
  	`end_ip` int(11) unsigned NOT NULL,
  	`country_code` char(2) NOT NULL,
  	`country_name` varchar(64) NOT NULL,
  	KEY `start_ip` (`start_ip`),
  	KEY `end_ip` (`end_ip`)
  )]]))
end

function insert_into_mysql(ip_start,ip_end,country_code,country_name)
	con:execute(("INSERT INTO ip2country (`start_ip`,`end_ip`,`country_code`,`country_name`)"..
	"VALUES ('%s','%s','%s','%s')"):format(ip_start,ip_end,country_code,country_name))
end

function LoadBase()
		local f = io.open(tdb["ip2country_db_file"],'r')
		local c = 0
		if f then
			for line in f:lines() do
				local ip1, ip2, country_code, country_name = line:match "\"(%d+)\",\"(%d+)\",\"%S+\",\"%d+\",\"(.*)\",\"%S+\",\"(.+)\""
				if ip1 then
				c = c +1 
				insert_into_mysql(ip1,ip2,country_code,country_name)
				end
			end
			f:close()
		end
	print(c)
end
 
con:execute("COMMIT")
assert(con:execute("OPTIMIZE TABLE ip2country"))
env:close()
con:close()

if arg and type(arg) == 'table' then
	mysql_table_create()
	LoadBase()
	connect_to_mysql()
end