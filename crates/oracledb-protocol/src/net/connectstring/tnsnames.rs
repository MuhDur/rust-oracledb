use crate::{ProtocolError, Result};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// A fully resolved set of tnsnames.ora entries.
#[derive(Debug, Default)]
pub struct TnsnamesReader {
    /// Alias (upper-cased) -> connect descriptor/easy-connect string, in
    /// first-seen order.
    entries: Vec<(String, String)>,
    /// The path of the primary tnsnames.ora file (for diagnostics).
    file_name: PathBuf,
}

impl TnsnamesReader {
    /// Reads `tnsnames.ora` from `config_dir`, following `IFILE` includes.
    pub fn read(config_dir: &Path) -> Result<Self> {
        let primary = config_dir.join("tnsnames.ora");
        let mut reader = TnsnamesReader {
            entries: Vec::new(),
            file_name: primary.clone(),
        };
        let mut in_progress: Vec<PathBuf> = Vec::new();
        let mut seen: HashSet<PathBuf> = HashSet::new();
        reader.read_file(&primary, &mut in_progress, &mut seen)?;
        Ok(reader)
    }

    /// Looks up an alias (case-insensitive). Returns the connect string.
    #[must_use]
    pub fn get(&self, alias: &str) -> Option<&str> {
        let upper = alias.to_ascii_uppercase();
        self.entries
            .iter()
            .find(|(name, _)| *name == upper)
            .map(|(_, value)| value.as_str())
    }

    /// All known network service names (upper-cased), in first-seen order.
    #[must_use]
    pub fn service_names(&self) -> Vec<String> {
        self.entries.iter().map(|(name, _)| name.clone()).collect()
    }

    /// The path of the primary tnsnames.ora file.
    #[must_use]
    pub fn file_name(&self) -> &Path {
        &self.file_name
    }

    fn set_entry(&mut self, name: String, value: String) {
        // Last definition wins, but keep first-seen ordering: if the alias
        // already exists, overwrite its value in place.
        if let Some(slot) = self.entries.iter_mut().find(|(n, _)| *n == name) {
            slot.1 = value;
        } else {
            self.entries.push((name, value));
        }
    }

    fn read_file(
        &mut self,
        path: &Path,
        in_progress: &mut Vec<PathBuf>,
        seen: &mut HashSet<PathBuf>,
    ) -> Result<()> {
        let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
        if in_progress.contains(&canonical) {
            let including = in_progress
                .last()
                .map(|p| p.display().to_string())
                .unwrap_or_default();
            return Err(ProtocolError::InvalidConnectDescriptor(format!(
                "file '{including}' includes file '{}', which forms a cycle",
                path.display()
            )));
        }
        let contents = std::fs::read_to_string(path).map_err(|_| {
            ProtocolError::InvalidConnectDescriptor(format!(
                "file '{}' is missing or unreadable",
                path.display()
            ))
        })?;
        in_progress.push(canonical.clone());
        seen.insert(canonical);

        let dir = path.parent().unwrap_or_else(|| Path::new("."));
        // Collect entries first to avoid borrow conflicts during IFILE
        // recursion.
        let parsed = parse_file(&contents);
        for (key, value) in parsed {
            if key.eq_ignore_ascii_case("ifile") {
                let mut inc = value.trim().to_string();
                if inc.starts_with('"') && inc.ends_with('"') && inc.len() >= 2 {
                    inc = inc[1..inc.len() - 1].to_string();
                }
                let inc_path = if Path::new(&inc).is_absolute() {
                    PathBuf::from(&inc)
                } else {
                    dir.join(&inc)
                };
                self.read_file(&inc_path, in_progress, seen)?;
            } else {
                // The key may be a comma-separated alias list spanning
                // multiple lines; split, take the last line of each, upper.
                for raw_alias in key.split(',') {
                    let alias = raw_alias.trim().lines().last().unwrap_or("").trim();
                    if alias.is_empty() {
                        continue;
                    }
                    self.set_entry(alias.to_ascii_uppercase(), value.clone());
                }
            }
        }
        in_progress.pop();
        Ok(())
    }
}

/// Parses a tnsnames.ora file into a list of `(key, value)` pairs, where the
/// key may be a (possibly multi-line) comma-separated alias list or `IFILE`,
/// and the value is the descriptor / easy-connect / include path. Mirrors
/// the reference `TnsnamesFileParser.parse`.
fn parse_file(contents: &str) -> Vec<(String, String)> {
    let chars: Vec<char> = contents.chars().collect();
    let mut parser = FileParser {
        chars: &chars,
        temp_pos: 0,
        pos: 0,
    };
    let mut out = Vec::new();
    while parser.temp_pos < parser.chars.len() {
        let key = parser.parse_key();
        let value = parser.parse_value();
        if let (Some(key), Some(value)) = (key, value) {
            if !key.is_empty() && !value.is_empty() {
                out.push((key, value.trim().to_string()));
            }
        }
    }
    out
}

/// Fuzz-only accessor for the in-memory tnsnames.ora lexer (`parse_file`).
///
/// Compiled **only** under `--cfg fuzzing` (set by `cargo-fuzz`); it never
/// widens the normal public API. It feeds arbitrary bytes through the
/// comment / multi-line / quote / paren-balancing tokenizer that the
/// `TnsnamesReader` runs on untrusted config files, so the connect-string
/// fuzz target can reach the tnsnames parser without touching the
/// filesystem (the `IFILE` recursion itself is I/O-bound and is covered by
/// `ifile_cycle_detected` / `ifile_same_directory`). Must never panic: the
/// lexer only returns a possibly-empty `(key, value)` list.
#[cfg(fuzzing)]
pub fn fuzz_parse_file(contents: &str) -> Vec<(String, String)> {
    parse_file(contents)
}

struct FileParser<'a> {
    chars: &'a [char],
    temp_pos: usize,
    pos: usize,
}

impl FileParser<'_> {
    fn current(&self) -> char {
        self.chars[self.temp_pos]
    }

    fn skip_spaces(&mut self) {
        while self.temp_pos < self.chars.len() && self.chars[self.temp_pos].is_whitespace() {
            self.temp_pos += 1;
        }
    }

    fn skip_to_end_of_line(&mut self) {
        while self.temp_pos < self.chars.len() {
            let ch = self.current();
            self.temp_pos += 1;
            if ch == '\n' || ch == '\r' {
                break;
            }
        }
        self.pos = self.temp_pos;
        self.skip_spaces();
    }

    /// Mirrors `_parse_key`: reads non-whitespace chars until `=`. Lines with
    /// stray parens / comments before `=` are discarded.
    fn parse_key(&mut self) -> Option<String> {
        let mut found_key = false;
        let mut start_pos = 0usize;
        self.skip_spaces();
        while self.temp_pos < self.chars.len() {
            let ch = self.current();
            if ch == '(' || ch == ')' || ch == '#' {
                self.skip_to_end_of_line();
                found_key = false;
                continue;
            } else if ch == '=' {
                if !found_key {
                    self.skip_to_end_of_line();
                    continue;
                }
                self.temp_pos += 1;
                self.pos = self.temp_pos;
                let key: String = self.chars[start_pos..self.temp_pos - 1].iter().collect();
                return Some(key.trim().to_string());
            } else if !found_key {
                found_key = true;
                start_pos = self.temp_pos;
            }
            self.temp_pos += 1;
        }
        None
    }

    /// Mirrors `_parse_value`: accumulates value parts until parens balance.
    fn parse_value(&mut self) -> Option<String> {
        let mut num_parens: isize = 0;
        let mut parts: Vec<String> = Vec::new();
        while self.temp_pos < self.chars.len() {
            if let Some(part) = self.parse_value_part(&mut num_parens) {
                parts.push(part);
            }
            if num_parens == 0 {
                break;
            }
        }
        if parts.is_empty() {
            None
        } else {
            Some(parts.join("\n"))
        }
    }

    /// Mirrors `_parse_value_part`.
    fn parse_value_part(&mut self, num_parens: &mut isize) -> Option<String> {
        let mut start_pos = 0usize;
        let mut end_pos = 0usize;
        let mut found_part = false;
        self.skip_spaces();
        while self.temp_pos < self.chars.len() {
            let ch = self.current();
            if ch == '#' {
                end_pos = self.temp_pos;
                self.skip_to_end_of_line();
                if found_part {
                    break;
                }
                continue;
            }
            if found_part && *num_parens == 0 {
                if ch == '\n' || ch == '\r' {
                    end_pos = self.temp_pos;
                    break;
                }
            } else if ch == '(' {
                *num_parens += 1;
            } else if ch == ')' && *num_parens > 0 {
                *num_parens -= 1;
            }
            if !found_part {
                found_part = true;
                start_pos = self.temp_pos;
            }
            self.temp_pos += 1;
            end_pos = self.temp_pos;
        }
        if found_part {
            let part: String = self.chars[start_pos..end_pos].iter().collect();
            Some(part.trim().to_string())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::net::connectstring::parse;

    use super::TnsnamesReader;
    use std::io::Write;

    /// Writes `contents` to `<dir>/<name>` and returns nothing.
    fn write_file(dir: &std::path::Path, name: &str, contents: &str) {
        let path = dir.join(name);
        let mut f = std::fs::File::create(&path).expect("create tns file");
        f.write_all(contents.as_bytes()).expect("write tns file");
    }

    fn temp_dir() -> std::path::PathBuf {
        let base = std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp".to_string());
        let unique = format!(
            "hk6_tns_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let dir = std::path::Path::new(&base).join(unique);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn resolves_simple_alias() {
        // reference test_7200
        let dir = temp_dir();
        write_file(
            &dir,
            "tnsnames.ora",
            "nsn_7200 = (DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=host_7200)(PORT=7200))\
             (CONNECT_DATA=(SERVICE_NAME=service_7200)))",
        );
        let reader = TnsnamesReader::read(&dir).expect("read tnsnames");
        let cs = reader.get("nsn_7200").expect("alias present");
        let d = parse(cs).unwrap().unwrap();
        let a = d.first_address().unwrap();
        assert_eq!(a.host.as_deref(), Some("host_7200"));
        assert_eq!(a.port, 7200);
    }

    #[test]
    fn missing_entry_is_none() {
        // reference test_7201
        let dir = temp_dir();
        write_file(&dir, "tnsnames.ora", "# no entries");
        let reader = TnsnamesReader::read(&dir).expect("read tnsnames");
        assert!(reader.get("nsn_7201").is_none());
        assert!(reader.service_names().is_empty());
    }

    #[test]
    fn missing_file_errors() {
        // reference test_7202
        let dir = temp_dir();
        let err = TnsnamesReader::read(&dir).unwrap_err();
        assert!(format!("{err}").contains("missing or unreadable"));
    }

    #[test]
    fn ignores_garbage_lines() {
        // reference test_7203
        let dir = temp_dir();
        write_file(
            &dir,
            "tnsnames.ora",
            "some garbage data which is not a valid entry\n\
             nsn_7203 = host_7203:7203/service_7203\n",
        );
        let reader = TnsnamesReader::read(&dir).expect("read tnsnames");
        assert!(reader.get("nsn_7203").is_some());
    }

    #[test]
    fn multiple_aliases_one_line() {
        // reference test_7204
        let dir = temp_dir();
        write_file(
            &dir,
            "tnsnames.ora",
            "nsn_7204a,nsn_7204b = host_7204:7204/service_7204\n",
        );
        let reader = TnsnamesReader::read(&dir).expect("read tnsnames");
        assert!(reader.get("nsn_7204a").is_some());
        assert!(reader.get("nsn_7204b").is_some());
        assert_eq!(reader.service_names(), vec!["NSN_7204A", "NSN_7204B"]);
    }

    #[test]
    fn case_insensitive_alias_lookup() {
        let dir = temp_dir();
        write_file(&dir, "tnsnames.ora", "Nsn_X = host:1521/svc\n");
        let reader = TnsnamesReader::read(&dir).expect("read tnsnames");
        assert!(reader.get("nsn_x").is_some());
        assert!(reader.get("NSN_X").is_some());
    }

    #[test]
    fn ifile_same_directory() {
        // reference test_7207
        let dir = temp_dir();
        write_file(&dir, "inc_7207.ora", "nsn_7207b = host_b:72072/service_b");
        write_file(
            &dir,
            "tnsnames.ora",
            "nsn_7207a = host_a:72071/service_a\nifile = inc_7207.ora",
        );
        let reader = TnsnamesReader::read(&dir).expect("read tnsnames");
        assert!(reader.get("nsn_7207a").is_some());
        assert!(reader.get("nsn_7207b").is_some());
    }

    #[test]
    fn ifile_cycle_detected() {
        // reference test_7209
        let dir = temp_dir();
        write_file(
            &dir,
            "tnsnames.ora",
            "nsn_7209 = some_host/some_service\nIFILE = tnsnames.ora",
        );
        let err = TnsnamesReader::read(&dir).unwrap_err();
        assert!(format!("{err}").contains("cycle"));
    }

    #[test]
    fn ifile_quoted_path() {
        // reference test_7223 style (double-quoted IFILE path)
        let dir = temp_dir();
        let inc = dir.join("inc_q.ora");
        write_file(&dir, "inc_q.ora", "nsn_q = host_q:1521/svc_q");
        write_file(
            &dir,
            "tnsnames.ora",
            &format!(
                "nsn_main = host_m:1521/svc_m\nifile = \"{}\"",
                inc.display()
            ),
        );
        let reader = TnsnamesReader::read(&dir).expect("read tnsnames");
        assert!(reader.get("nsn_q").is_some());
    }

    #[test]
    fn duplicate_entry_last_wins() {
        // reference test_7213
        let dir = temp_dir();
        write_file(
            &dir,
            "tnsnames.ora",
            "nsn = host_a:7213/svc_a\nother = h/s\nnsn = host_b:7213/svc_b\n",
        );
        let reader = TnsnamesReader::read(&dir).expect("read tnsnames");
        let d = parse(reader.get("nsn").unwrap()).unwrap().unwrap();
        assert_eq!(d.first_address().unwrap().host.as_deref(), Some("host_b"));
    }

    #[test]
    fn multiline_aliases() {
        // reference test_7219
        let dir = temp_dir();
        write_file(
            &dir,
            "tnsnames.ora",
            "nsn_a,\nnsn_b,\nnsn_c = host:1521/svc",
        );
        let reader = TnsnamesReader::read(&dir).expect("read tnsnames");
        assert!(reader.get("nsn_a").is_some());
        assert!(reader.get("nsn_b").is_some());
        assert!(reader.get("nsn_c").is_some());
    }

    #[test]
    fn embedded_comment_in_descriptor() {
        // reference test_7220
        let dir = temp_dir();
        write_file(
            &dir,
            "tnsnames.ora",
            "nsn_7220 = (DESCRIPTION=\n(ADDRESS=(PROTOCOL=TCP)(HOST=host_7220)(PORT=7220))\n\
             (CONNECT_DATA=\n(SERVICE_NAME=service_7220)\n# embedded comment\n)\n)\n",
        );
        let reader = TnsnamesReader::read(&dir).expect("read tnsnames");
        let d = parse(reader.get("nsn_7220").unwrap()).unwrap().unwrap();
        assert_eq!(
            d.first_address().unwrap().host.as_deref(),
            Some("host_7220")
        );
    }

    #[test]
    fn missing_ifile_errors() {
        // reference test_7216
        let dir = temp_dir();
        write_file(&dir, "tnsnames.ora", "IFILE = missing.ora\n");
        let err = TnsnamesReader::read(&dir).unwrap_err();
        assert!(format!("{err}").contains("missing or unreadable"));
    }

    // bead rust-oracledb-uf8: a deeply-nested descriptor must return a clean
    // Err, never recurse until the stack overflows and ABORTS the process.
    #[test]
    fn deeply_nested_descriptor_errors_not_crashes() {
        // 5000 levels of "(A=" + "1" + 5000 ")" — far past MAX_DESCRIPTOR_DEPTH
        // but small enough that the depth guard fires long before any real
        // stack pressure. Without the guard this overflows the stack.
        let depth = 5000;
        let mut s = String::with_capacity(depth * 4);
        for _ in 0..depth {
            s.push_str("(A=");
        }
        s.push('1');
        for _ in 0..depth {
            s.push(')');
        }
        let err = parse(&s).unwrap_err();
        assert!(
            format!("{err}").contains("nesting too deep"),
            "expected a nesting-depth error, got: {err}"
        );
    }

    #[test]
    fn legitimately_deep_descriptor_still_parses() {
        // A realistic DESCRIPTION_LIST topology (~5 deep) must NOT be rejected.
        let ok = "(DESCRIPTION_LIST=(DESCRIPTION=(ADDRESS_LIST=\
                  (ADDRESS=(PROTOCOL=tcp)(HOST=h)(PORT=1521)))\
                  (CONNECT_DATA=(SERVICE_NAME=svc))))";
        assert!(parse(ok).is_ok(), "a real ~5-deep descriptor must parse");
    }
}
