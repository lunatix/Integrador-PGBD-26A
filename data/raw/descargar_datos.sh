wget https://www.secciones.hacienda.gob.mx/work/models/estadisticas_oportunas/datos_abiertos_eopf/ingreso_gasto_finan.csv --no-check-certificate
wget https://wu1agsprosta001.blob.core.windows.net/agsc-publicaciones/Datos_abiertos/Cifras_SAT/Documents/IngresosTributarios.xls --no-check-certificate
wget https://www.inegi.org.mx/contenidos/programas/igae/2018/datosabiertos/igae_mensual_csv.zip --no-check-certificate
wget https://www.inegi.org.mx/contenidos/programas/inpc/2018aedatosabiertos/conjunto_de_datos_inpc_indicador_mensual_csv.zip --no-check-certificate

mkdir igf
mkdir it

unzip igae_mensual_csv.zip -d igae
unzip conjunto_de_datos_inpc_indicador_mensual_csv.zip -d inpc

mv ingreso_gasto_finan.csv igf/igf.csv 
mv IngresosTributarios.xls it/it.xls

