defmodule MerklePatriciaTree.DestroyerTest do
  use ExUnit.Case, async: true
  doctest MerklePatriciaTree.Trie.Destroyer

  alias MerklePatriciaTree.Trie
  alias MerklePatriciaTree.Trie.Builder
  alias MerklePatriciaTree.Trie.Destroyer
  alias MerklePatriciaTree.Trie.Node
  alias MerklePatriciaTree.DB.ETS

  setup do
    db = ETS.random_ets_db()
    trie = Trie.new(db)

    {:ok, %{trie: trie}}
  end

  defp store_node(node, trie) do
    Node.encode_node(node, trie)
      |> Trie.into(trie)
      |> Trie.store
  end

  test "Remove exact leaf from trie with only the leaf", %{trie: trie} do
    key = [1, 2, 3]
    value = "value"

    node = Node.decode_trie(trie)
      |> Builder.put_key(key, value, trie)

    one_leaf_trie = store_node(node, trie)

    after_removal = Destroyer.remove_key(node, key, one_leaf_trie)

    assert after_removal == :empty
  end

  test "Remove non-existed key from trie with only a leaf", %{trie: trie} do
    key = [1, 2, 3]
    value = "value"

    node = Node.decode_trie(trie)
      |> Builder.put_key(key, value, trie)

    one_leaf_trie = store_node(node, trie)

    after_removal = Destroyer.remove_key(node, [4], one_leaf_trie)

    assert after_removal == node
  end

  test "Remove key whose value is directly on branch", %{trie: trie} do
    key = [1, 2, 3]
    value = "value"

    one_leaf_trie = Node.decode_trie(trie)
      |> Builder.put_key(key, value, trie)
      |> store_node(trie)

    key2 = [1, 2]
    value2 = "value2"
    {:ext, _prefix, _branch_hash} = ext_node = Node.decode_trie(one_leaf_trie)
      |> Builder.put_key(key2, value2, one_leaf_trie)

    two_leaf_trie = ext_node
      |> store_node(one_leaf_trie)

    after_removal = Destroyer.remove_key(ext_node, key2, two_leaf_trie)

    assert after_removal == {:leaf, key, value}
  end

  test "Remove key whose value is beneath branch and leaving only one leaf", %{trie: trie} do
    key = [1, 2, 3]
    value = "value"

    one_leaf_trie = Node.decode_trie(trie)
      |> Builder.put_key(key, value, trie)
      |> store_node(trie)

    key2 = [1, 2]
    value2 = "value2"
    {:ext, _prefix, _branch_hash} = ext_node = Node.decode_trie(one_leaf_trie)
      |> Builder.put_key(key2, value2, one_leaf_trie)

    two_leaf_trie = ext_node
      |> store_node(one_leaf_trie)

    after_removal = Destroyer.remove_key(ext_node, key, two_leaf_trie)

    assert after_removal == {:leaf, key2, value2}
  end

  test "Remove key sharing nothing with the ext prefix", %{trie: trie} do
    key = [1, 2, 3]
    value = "value"

    one_leaf_trie = Node.decode_trie(trie)
      |> Builder.put_key(key, value, trie)
      |> store_node(trie)

    key2 = [1, 2]
    value2 = "value2"
    {:ext, _prefix, _branch_hash} = ext_node = Node.decode_trie(one_leaf_trie)
      |> Builder.put_key(key2, value2, one_leaf_trie)

    two_leaf_trie = ext_node
      |> store_node(one_leaf_trie)

    after_removal = Destroyer.remove_key(ext_node, [3], two_leaf_trie)

    assert after_removal == ext_node
  end

  test "Remove key from trie containing a ext node and branch with 3 nodes", %{trie: trie} do
    # Prepare trie
    key = [1, 2]
    value = "value"

    one_leaf_trie = Node.decode_trie(trie)
      |> Builder.put_key(key, value, trie)
      |> store_node(trie)

    key2 = [1, 2, 3, 4]
    value2 = "value2"

    ext_node = Node.decode_trie(one_leaf_trie)
      |> Builder.put_key(key2, value2, one_leaf_trie)

    {:ext, _ext_prefix, _branch_hash} = ext_node

    two_leaf_trie = ext_node
      |> store_node(one_leaf_trie)

    # Puts a new key which is removed later
    key3 = [1, 2, 5]
    value3 = "value3"
    {:ext, _ext_prefix, _updated_branch_hash} = updated_ext_node = Node.decode_trie(two_leaf_trie)
      |> Builder.put_key(key3, value3, two_leaf_trie)

    three_leaf_trie = updated_ext_node
      |> store_node(two_leaf_trie)

    # Remove just put new key
    after_removal_ext = Destroyer.remove_key(updated_ext_node, key3, three_leaf_trie)
    assert after_removal_ext == ext_node
  end

  test "Remove key from a branch beneath a branch which produces an ext node", %{trie: trie} do
    # Prepare trie
    key = [1]
    value = "value"

    one_leaf_trie = Node.decode_trie(trie)
      |> Builder.put_key(key, value, trie)
      |> store_node(trie)

    key2 = [1, 2]
    value2 = "value2"

    ext_node = Node.decode_trie(one_leaf_trie)
      |> Builder.put_key(key2, value2, one_leaf_trie)

    {:ext, _ext_prefix, _branch_hash} = ext_node

    two_leaf_trie = store_node(ext_node, one_leaf_trie)

    # Update prepared ext node with key sharing nothing
    # Produce a new branch node with a branch node directly under it
    key3 = [5]
    value3 = "value3"
    old_ext_node = {:ext, _prefix, decoded_old_branch } = Node.decode_trie(two_leaf_trie)

    new_branch_node = {:branch, _branch_options} = Builder.put_key(old_ext_node, key3, value3, two_leaf_trie)
    three_leaf_trie = store_node(new_branch_node, two_leaf_trie)

    # Remove newly added key produce an ext node with original branch node
    after_removal_ext = Destroyer.remove_key(new_branch_node, key3, three_leaf_trie)
    assert after_removal_ext == {:ext, key, decoded_old_branch}
  end

  test "Remove key from a branch percolating up the ext node beneath it", %{trie: trie} do
    # Prepare trie
    key = [1, 2]
    value = "value"

    one_leaf_trie = Node.decode_trie(trie)
      |> Builder.put_key(key, value, trie)
      |> store_node(trie)

    key2 = [1, 2, 3]
    value2 = "value2"

    ext_node = Node.decode_trie(one_leaf_trie)
      |> Builder.put_key(key2, value2, one_leaf_trie)

    {:ext, matching_prefix, _branch_hash} = ext_node
    assert matching_prefix == key

    two_leaf_trie = store_node(ext_node, one_leaf_trie)

    # Update prepared ext node with key sharing nothing
    # Produce a new branch node with an ext node under it
    key3 = [5, 6]
    value3 = "value3"
    old_ext_node = {:ext, _prefix, _decoded_old_branch } = Node.decode_trie(two_leaf_trie)

    new_branch_node = {:branch, _branch_options} = Builder.put_key(old_ext_node, key3, value3, two_leaf_trie)
    three_leaf_trie = store_node(new_branch_node, two_leaf_trie)

    # Remove newly added key produce an ext node with original branch node
    after_removal_ext = Destroyer.remove_key(new_branch_node, key3, three_leaf_trie)
    assert after_removal_ext == old_ext_node
  end

  test "Remove key from an ext percolating up the ext node beneath it", %{trie: trie} do
    # Prepare trie
    key = [1, 2]
    value = "value"

    one_leaf_trie = Node.decode_trie(trie)
      |> Builder.put_key(key, value, trie)
      |> store_node(trie)

    key2 = [1, 2, 3, 4, 5]
    value2 = "value2"

    two_leaf_trie = Node.decode_trie(one_leaf_trie)
      |> Builder.put_key(key2, value2, one_leaf_trie)
      |> store_node(one_leaf_trie)

    key3 = [1, 2, 3, 4, 6]
    value3 = "value3"
    ext_node = Node.decode_trie(two_leaf_trie)
      |> Builder.put_key(key3, value3, two_leaf_trie)
    three_leaf_trie = store_node(ext_node, two_leaf_trie)

    # Remove key of the branch percolating up the ext node beneath it
    {:ext, new_ext_prefix, new_branch_hash} = Destroyer.remove_key(ext_node, key, three_leaf_trie)
    assert new_ext_prefix == [1, 2, 3, 4]
    assert {:leaf, [], value2} == ExRLP.decode(Enum.at(new_branch_hash, 5))
      |> Node.decode_node(two_leaf_trie)
    assert {:leaf, [], value3} == ExRLP.decode(Enum.at(new_branch_hash, 6))
      |> Node.decode_node(two_leaf_trie)
  end
end
