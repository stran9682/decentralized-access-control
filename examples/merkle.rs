use anyhow::Context;
use rs_merkle::{Hasher, MerkleTree, algorithms::Sha256};

fn main() -> anyhow::Result<()> {
    let mut leaves: Vec<[u8; 32]> = Vec::new();
    for i in 'a'..'z' {
        leaves.push(Sha256::hash(&[i as u8]));
    }

    let merkle_tree = MerkleTree::<Sha256>::from_leaves(&leaves);

    let indices_to_prove = vec![3];
    let leaves_to_prove = vec![leaves[3]];

    let proof = merkle_tree.proof(&indices_to_prove);
    let root = merkle_tree.root().context("couldn't get the merkle root")?;

    assert!(proof.verify(root, &indices_to_prove, &leaves_to_prove, leaves.len()));

    Ok(())
}
