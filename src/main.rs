use sqlparser::ast::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use std::error;
use std::fmt;
use std::cell::RefCell;
use traversal::DftPre;
use std::thread::LocalKey;

// Errors
// We can wrap ParserError with a custom QueryParseError. 

#[derive(Debug)]
enum QueryParseError {
    InvalidNode,
    NotImplemented,
    Parse(ParserError)
}

impl fmt::Display for QueryParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            QueryParseError::NotImplemented =>
                write!(f, "not implemented yet"),
            QueryParseError::InvalidNode =>
            write!(f, "invalid node"), 
            QueryParseError::Parse(e) =>
                write!(f, "parse: {:?}", e),
        }
    }
}

impl From<ParserError> for QueryParseError {
    fn from(err: ParserError) -> QueryParseError {
        QueryParseError::Parse(err)
    }
}

impl error::Error for QueryParseError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            QueryParseError::NotImplemented | QueryParseError::InvalidNode => None,
            QueryParseError::Parse(ref e) => Some(e),
        }
    }
}

// Tree

/// AST nodes
#[derive(Clone, Debug)]
enum Node {
    BinaryOperator(BinaryOperator),
    Expr(Expr),
    Query(Query),
    SetExpr(SetExpr),
    Select(Select),
    SelectItem(SelectItem),
    Statement(Statement),
    TableWithJoins(TableWithJoins),
}

// Storage for a reference to the next node in stm_iter.
// We can't return the node position (usize) itself as `traversal`
// insists to return a reference. We also can't return a reference
// to an element on the stack as it wouldn't live long enough.
// So we allocate the NEXT storage on the heap, put an element there
// and return a reference to this storage.
thread_local!(static NEXT: RefCell<usize> = RefCell::new(0));
// Storage for our custom AST nodes. We can't make it a part of the
// stm_iterator because of the borrow checker. It believes that in
// this case iterator "returns a reference to a captured variable which
// escapes the closure body". Though I suggest it is a false positive
// error, we can't make the code compile. So, global storage for the
// nodes outside of the iterator structure is a solution.
thread_local!(static NODES: RefCell<Nodes> = RefCell::new(Nodes::new()));

fn next_put(id: usize) {
    NEXT.with(|rc_id| { *rc_id.borrow_mut() = id; })
}

fn next_get() -> usize {
    NEXT.with(|rc_id| { *rc_id.borrow() })
}

fn nodes_next_id() -> usize {
    NODES.with(|rc_nodes| {
        rc_nodes.borrow().next_id()
    })
}

#[derive(Debug)]
struct Nodes {
    arena: Vec<Node>,
}

/// Iterator over statement node's children
struct StatementIterator {
    /// current node id in the NODES list
    current: usize,
    /// keep the state
    step: RefCell<usize>,
}

impl Nodes {
    fn new() -> Self {
        Nodes {
            arena: Vec::new()
        }
    }

    fn next_id(&self) -> usize {
        self.arena.len()
    }

    
    fn new_node(&mut self, node: Node) -> usize {
        let id = self.next_id();
        self.arena.push(node);
        id
    }
}

/// Statement iterator constructor
fn stm_iter<'n>(node_ptr: &'static LocalKey<RefCell<usize>>) -> StatementIterator {
    let current = node_ptr.with(|p| {*p.borrow()});
    StatementIterator {
        current,
        step: RefCell::new(0),
    }
}


impl Iterator for StatementIterator {
    type Item = &'static LocalKey<RefCell<usize>>;

    fn next(&mut self) -> Option<Self::Item> {
        let node: Option<Node> =  NODES.with(|rc_nodes| {
            match rc_nodes.borrow().arena.get(self.current) {
                Some(node) => {
                    Some(node.clone())
                },
                _ => None, 
            }
        });

        let new_node = |node: Node| -> () {
            *self.step.borrow_mut() += 1;
            let id = nodes_next_id();
            NODES.with(|rc_nodes| {
                rc_nodes.borrow_mut().new_node(node);
            });
            next_put(id); 
        };

        match node {
            Some(Node::Statement(stm)) => {
                match stm {
                    Statement::Query(query) => {
                        let step = *self.step.borrow();
                        if step == 0 {
                            new_node(Node::SetExpr(query.body.clone()));
                            return Some(&NEXT)
                        }
                        return None;
                    },
                    // TODO: Insert
                    _ => return None,
                }
            },
            Some(Node::SetExpr(set_expr)) => {
                match set_expr {
                    SetExpr::Select(select) => {
                        let step = *self.step.borrow();
                        // Iterate "from"
                        if step < select.from.len() {
                            new_node(Node::TableWithJoins(select.from[step].clone()));
                            return Some(&NEXT) 
                        }
                        // TODO: iterate projection, selection
                        return None;
                    },
                    // TODO: Query, SetOperation, Values, Insert
                    _ => return None,
                }
            }
            // TODO: other nodes
            _ => return None, 
        }
    }
}

// Main

fn main() {
    let query = "select a, b from t where a = 1";
    parse_sql(query).unwrap();
}

fn parse_sql(sql: &str) -> Result<(), QueryParseError> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;
    for stm in statements {
        println!("{:?}", stm);
        let top = nodes_next_id();
        NODES.with(|rc_nodes| {
            rc_nodes.borrow_mut().new_node(Node::Statement(stm));
        });
        next_put(top);
        let dft_pre = DftPre::new(&NEXT, |node| stm_iter(node));
        for (_level, node) in dft_pre {
            let id = node.with(|p| { *p.borrow() } );
            NODES.with(|rc_nodes| {
                if let Some(node) = rc_nodes.borrow().arena.get(id) {
                    println!("{:?}", node);
                }
            });
        }
    }
    Ok(())
}
