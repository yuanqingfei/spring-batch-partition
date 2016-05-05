package cn.com.mall;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

public class CategoryPartitioner implements Partitioner {

	private Logger logger = Logger.getLogger(CategoryPartitioner.class);

	private JdbcOperations jdbcTemplate;

	public CategoryPartitioner(DataSource dataSource) {
		this.jdbcTemplate = new JdbcTemplate(dataSource);
	}

	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		logger.info("begin partition");
		List<String> brandIds = jdbcTemplate.queryForList(
				"select distinct(category) from my_product;", String.class);
		Map<String, ExecutionContext> results = new LinkedHashMap<String, ExecutionContext>();
		for (String brandId : brandIds) {
			ExecutionContext context = new ExecutionContext();
			context.put("category", brandId);
			results.put("partition." + brandId, context);
		}
		return results;
	}

}
