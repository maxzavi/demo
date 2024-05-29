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
		saveDB(datos);
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

	private void saveDB(List<Asn> asns){
		String query = """
			INSERT INTO WmsShipmentMainApi
			(shipment_id, facility_key, shipment_nbr, create_ts, mod_ts, shipment_type_id, shipment_type_key)
			VALUES(?, ?, ?, ?, ?, ?, ?)
				""";
		try{ 
            Connection con = jdbcTemplate.getDataSource().getConnection();
            PreparedStatement pst = con.prepareStatement(query);
        
			//con.setAutoCommit(false);
			for (Asn t : asns) {
				pst.setString(1, t.shipment_id());
				pst.setString(2, t.facility_key());
				pst.setString(3, t.shipment_nbr());
				pst.setString(4, t.create_ts().substring(0,20));
				pst.setString(5, t.mod_ts().substring(0,20));
				pst.setInt(6, t.shipment_type_id());
				pst.setString(7, t.shipment_type_key());

				pst.addBatch();
			
			}
			pst.executeBatch();

			pst.close();
			con.close();
			//con.commit();
        } catch (Exception e) {
            log.error(e);
            //throw e;
        }
	}
	

}
