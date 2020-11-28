using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Net;

namespace CovidETL
{
    class Program
    {
        static String connectionString = @"Data Source=DESKTOP-MRNQERB\SQLEXPRESS;Initial Catalog=coronavirus;Integrated Security=True";

        static void Main(string[] args)
        {     
            List<Pais> datos = DescargarData(@"https://pomber.github.io/covid19/timeseries.json");        
            if (VaciarTabla()) { EnviarDataBBDD(datos); }
        }

        private static bool VaciarTabla()
        {           
            using (SqlConnection openCon = new SqlConnection(connectionString))
            {
                string stringConsulta = "truncate table[datos_generales]";
                using (SqlCommand query = new SqlCommand(stringConsulta))
                {
                    try
                    {
                        query.Connection = openCon;
                        openCon.Open();
                        query.ExecuteNonQuery();
                        Console.WriteLine("Tabla truncada");
                        openCon.Close();
                    }
                    catch (Exception)
                    {
                        openCon.Close();
                        Console.WriteLine("Error al truncar");
                        return false;
                    }
                }             
            }
            return true;
        }

        private static bool EnviarDataBBDD(List<Pais> datos)
        {
            using (SqlConnection openCon = new SqlConnection(connectionString))
            {
                for (int pais = 0; pais < datos.Count; pais++)
                {
                    for (int d = 0; d < datos[pais].datos.Count; d++)
                    {
                        string stringConsulta = "INSERT into datos_generales (pais, fecha, confirmados, muertos, recuperados) VALUES ('" + datos[pais].name + "','" + datos[pais].datos[d].date + "'," + datos[pais].datos[d].confirmed + ", " + datos[pais].datos[d].deaths + ", " + datos[pais].datos[d].recovered + ")";

                        using (SqlCommand query = new SqlCommand(stringConsulta))
                        {
                            try
                            {
                                query.Connection = openCon;
                                openCon.Open();
                                query.ExecuteNonQuery();
                                Console.WriteLine("Enviado: " + stringConsulta);
                                openCon.Close();
                            }
                            catch (Exception)
                            {
                                openCon.Close();
                                Console.WriteLine("Existe o error, continuando...");                               
                            }                      
                        }
                    }                    
                }               
            }
            return true;
        }

        private static List<Pais> DescargarData(string url)
        {
            List<Pais> datos = new List<Pais>();

            using (var w = new WebClient())
            {
                var json_data = w.DownloadString(url);
                JObject ob = JsonConvert.DeserializeObject<JObject>(json_data);               

                foreach (var x in ob)
                {
                    Pais p = new Pais();
                    p.name = x.Key.Replace("'","");
                    p.datos = JsonConvert.DeserializeObject<List<Datos>>(x.Value.ToString().Replace("null", "0"));
                    datos.Add(p);
                }
            }
            return datos;
        }
    }

    public class Pais
    {
        public String name;
        public List<Datos> datos;
    }

    public class Datos
    {
        public DateTime date { get; set; }
        public int confirmed { get; set; }
        public int deaths { get; set; }
        public int recovered { get;set; }   
    }   
}



