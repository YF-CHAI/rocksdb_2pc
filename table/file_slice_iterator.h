#pragma once
#include "internal_iterator.h"
#include "db/version_edit.h"
#include "db/dbformat.h"
#include "rocksdb/status.h"

namespace rocksdb {
	class FileSliceIterator : public InternalIterator{
		public:
			explicit FileSliceIterator(const FileSlice& file_slice, InternalIterator* file_iter, 
				const InternalKeyComparator& internal_comparator);

			~FileSliceIterator() { delete file_iter_; }

			virtual bool Valid() const;

			virtual Slice key() const;

			virtual Slice value() const;

			virtual Status status() const;

			virtual void Next();

			virtual void Prev();

			virtual void Seek(const Slice& target);

			virtual void SeekToFirst();

			virtual void SeekToLast();

			virtual void SeekForPrev(const Slice& target);

			virtual void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr);

			virtual bool IsKeyPinned() const;

			virtual bool IsValuePinned() const;

			virtual Status GetProperty(std::string prop_name, std::string* prop);
		private:
			const InternalKeyComparator icmp_;
			InternalIterator* file_iter_;
			const FileSlice file_slice_;
	};
}