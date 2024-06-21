#include "rm_scan.h"

#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 *
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）

    // 初始化file_handle_
    this->file_handle_ = file_handle;

    // 记录id初始化为首记录
    this->rid_.page_no =  RM_FIRST_RECORD_PAGE;
    this->rid_.slot_no = -1;

    // 找到第一条记录的页号和槽号
    next();

}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置
    while(this->rid_.page_no < file_handle_ -> file_hdr_.num_pages){
        RmPageHandle page_handle = file_handle_->fetch_page_handle(this->rid_.page_no);
        this->rid_.slot_no = Bitmap::next_bit(true, page_handle.bitmap,
                                              file_handle_->file_hdr_.num_records_per_page,
                                              this->rid_.slot_no);
        if(this->rid_.slot_no >= this->file_handle_->file_hdr_.num_records_per_page){  //本页没有
            if ((this->rid_.page_no + 1) == file_handle_ -> file_hdr_.num_pages){ // 遍历后续所有页，未找到
                this->rid_ = Rid{RM_NO_PAGE, -1};
                break;
            }
            else{
                this->rid_ = Rid{this->rid_.page_no+1, -1};  //获取下一页，下一个循环继续找下一页
            }
        }
        else{
            break;
        }
    }
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值
//    bool flag= false;
//    if(this->rid_.page_no == ( file_handle_->file_hdr_.num_pages - 1 )){  // 最后一页
//        auto now_page = file_handle_->fetch_page_handle(rid_.page_no);  // 当前页
//        if(this->rid_.slot_no == ( Bitmap::first_bit(false, now_page.bitmap, file_handle_->file_hdr_.num_records_per_page) - 1 )){  // 当前页第一个空闲slot的前一个
//            flag= true;
//        }
//    }
//    return flag;
    return rid_.page_no == RM_NO_PAGE;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    // Todo: 修改返回值
    return this->rid_;
}