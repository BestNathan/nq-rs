use anyhow::Result;

#[allow(dead_code)]
struct Response {
    result: Vec<Vec<f64>>,
}

#[allow(dead_code)]
fn response_to_lines(resp: Response) -> Result<Vec<String>> {
    let mut strs: Vec<String> = vec![];
    for row in resp.result {
        strs.push(format!(
            "deribit_rv,index_name=ETH rv={} {}000000",
            row.get(1).map(|f| f.to_string()).unwrap_or_default(),
            row.first().map(|f| f.to_string()).unwrap_or_default(),
        ));
    }
    Ok(strs)
}
