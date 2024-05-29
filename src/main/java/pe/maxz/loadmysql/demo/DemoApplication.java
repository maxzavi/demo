package pe.maxz.loadmysql.demo;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;

import lombok.extern.log4j.Log4j2;

@Log4j2
@SpringBootApplication
public class DemoApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		var datos = getData();
		log.info(datos.size());
		saveDB1(datos);
	}

	@Autowired
	private JdbcTemplate jdbcTemplate;

	public List<Asn> getData() throws URISyntaxException, IOException{
		List<Asn> temp = new ArrayList<Asn>();
		Path path = Paths.get(getClass().getClassLoader()
      		.getResource("data.in").toURI());
			
		Stream<String> lines = Files.lines(path);
		lines.forEach(l->{
			//log.info(l);
			var dato = l.split(",");
			String type_key ="";
			if (dato.length==6) type_key=dato[5];
			//log.info(dato[0]+ ","+ dato[1]+ dato[2]+dato[3]+dato[4]+ type_key);
			var asn = new Asn(dato[1] + dato[0], dato[0], dato[1], dato[2], dato[3], Integer.parseInt( dato[4]), type_key);
			temp.add(asn);
		});	
		return temp;
	}
	
	private void saveDB1(List<Asn> asns){
		StringBuilder sb = new StringBuilder();
		boolean firstRow=true;
		for (Asn t : asns) {
			if (firstRow){
				firstRow=false;
			}else{
				sb.append(" union all ");
			}			
			sb.append("select '" + t.shipment_id() + "'");
			sb.append(",'" + t.facility_key() +"'");
			sb.append(",'" + t.shipment_nbr() +"'");
			sb.append(",'" + t.create_ts().substring(0,19) +"'");
			sb.append(",'" + t.mod_ts().substring(0,19) +"'");
			sb.append("," + t.shipment_type_id() );
			sb.append(",'" + t.shipment_type_key() +"' ");
		}

		String query = """
			insert into WmsShipmentMainApi
			(shipment_id, facility_key, shipment_nbr, create_ts, mod_ts, shipment_type_id, shipment_type_key)
				""";
		query= query + sb.toString();
		try(
            Connection con = jdbcTemplate.getDataSource().getConnection();
            PreparedStatement pst = con.prepareStatement(query);
		){ 
			pst.executeUpdate();
        } catch (Exception e) {
            log.error(e);
            //throw e;
        }
	}
}
