#include "file_slice_iterator.h"
#include "internal_iterator.h"
#include "db/version_edit.h"
#include "db/dbformat.h"
#include "util/sync_point.h"
#include <iostream>

namespace rocksdb{

FileSliceIterator::FileSliceIterator(const FileSlice& file_slice, InternalIterator* file_iter, 
			const InternalKeyComparator& internal_comparator)
	:	icmp_(internal_comparator),
		file_iter_(file_iter),
		file_slice_(file_slice) {
		SeekToFirst();
		//size_ = 0;
}

bool FileSliceIterator::Valid() const{
	if (!file_iter_->Valid()){
		//std::cout << "FileSliceIterator::Valid false !file_iter_->Valid() size:" << *file_slice_size_ << std::endl;
		return false;
	}

	Slice key = file_iter_->key();
	auto scmp = icmp_.Compare(key, file_slice_.smallest.Encode());
	if (scmp < 0 || (scmp == 0 && !file_slice_.is_contain_smallest)){
		return false;
	}

	if (icmp_.Compare(key, file_slice_.largest.Encode()) > 0){
		//std::cout << "FileSliceIterator::Valid false largest size:" << *file_slice_size_ << std::endl;
		return false;
	}

	return true;
}

Slice FileSliceIterator::key() const{
	assert(Valid());
	return file_iter_->key();
}

Slice FileSliceIterator::value() const{
	assert(Valid());
	return file_iter_->value();
}

void FileSliceIterator::Next() {
	if (Valid()) {
		std::string key_str = key().ToString();
		Slice pre_key(key_str);
		rocksdb::TwoPCStatic::GetInstance()->compaction_input_size += key().size() + value().size();
		file_iter_->Next();
		if (Valid()) {
			if (icmp_.Compare(key(), pre_key) <= 0) {
				std::cout << "pre_key:" << pre_key.ToString() << " key:" << key().ToString() << std::endl;
			}
			assert(icmp_.Compare(key(), pre_key) > 0);
		}
	}
}

void FileSliceIterator::Prev() {
	if(Valid()) {
		file_iter_->Prev();
	}
}

//TODO WEIHAOCHENG: add error status
Status FileSliceIterator::status() const {
	return Status::OK();
}

void FileSliceIterator::Seek(const Slice& target) {
	file_iter_->Seek(target);
}

void FileSliceIterator::SeekToFirst() {
	file_iter_->Seek(file_slice_.smallest.Encode());
	if (!file_slice_.is_contain_smallest && file_iter_->Valid() && icmp_.Compare(file_iter_->key(), file_slice_.smallest.Encode()) == 0) {
		file_iter_->Next();
	}
}

void FileSliceIterator::SeekToLast() {
	file_iter_->Seek(file_slice_.largest.Encode());
}

void FileSliceIterator::SeekForPrev(const Slice& target) {
	file_iter_->SeekForPrev(target);
}

void FileSliceIterator::SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) {
	//assert(false);
	file_iter_->SetPinnedItersMgr(pinned_iters_mgr);
}

bool FileSliceIterator::IsKeyPinned() const { 
	return file_iter_->IsKeyPinned(); 
}

bool FileSliceIterator::IsValuePinned() const { 
	return file_iter_->IsValuePinned(); 
}

Status FileSliceIterator::GetProperty(std::string prop_name, std::string* prop) {
    return Status::NotSupported("");
}

} //namespace rocksdb