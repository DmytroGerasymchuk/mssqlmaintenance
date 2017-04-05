{
	sub(/\$\$\$ROOTPATH\$\$\$/, rootpath);
	sub(/MSSQL\$MSSQLSERVER\_XXX/, sqlprefix);
	print;
}