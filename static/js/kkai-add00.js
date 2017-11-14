
// Operate CRUD: Create
Array.prototype.kk_insert_at = function (index, item) {
  if ((index <  0) || (index >= this.length)) {
    this.push(item);
  }
  else {
    this.splice(index, 0, item);
  }
};

// Operate CRUD: Read

// Operate CRUD: Update
Array.prototype.kk_update_at = function (index, item) {
  if ((index >= 0) && (index <  this.length)) {
    this.splice(index, 1, item);
  }
};

// Operate CRUD: Delete
Array.prototype.kk_delete_at = function (index) {
  if ((index < 0) || (index >= this.length)) {
    this.pop();
  }
  else {
    this.splice(index, 1);
  }
};

