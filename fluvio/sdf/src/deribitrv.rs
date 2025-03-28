use anyhow::Result;

struct Response {
    result: Vec<Vec<f64>>,
}

fn response_to_lines(resp: Response) -> Result<Vec<String>> {
    let mut strs: Vec<String> = vec![];
    for row in resp.result {
        strs.push(format!(
            "{},{}",
            row.get(0).map(|f| f.to_string()).unwrap_or_default(),
            row.get(1).map(|f| f.to_string()).unwrap_or_default()
        ));
    }
    Ok(strs)
}
