import { ChevronLeftIcon, ChevronRightIcon } from '@heroicons/react/20/solid'

//Logic borrowed from https://www.freecodecamp.org/news/build-a-custom-pagination-component-in-react/
const range = (start: number, end: number) => {
    let length = end - start + 1;
    /*
        Create an array of certain length and set the elements within it from
      start value to end value.
    */
    return Array.from({ length }, (_, idx) => idx + start);
};

const DOTS1 = "DOTS1";
const DOTS2 = "DOTS2";

const paginationRange = (currentPage: number, siblingCount: number, totalPageCount: number, totalPageNumbers: number) => {
    /*
      Case 1:
      If the number of pages is less than the page numbers we want to show in our
      paginationComponent, we return the range [1..totalPageCount]
    */
      if (totalPageNumbers >= totalPageCount) {
        return range(1, totalPageCount);
    }

    /*
        Calculate left and right sibling index and make sure they are within range 1 and totalPageCount
    */
    const leftSiblingIndex = Math.max(currentPage - siblingCount, 1);
    const rightSiblingIndex = Math.min(
        currentPage + siblingCount,
        totalPageCount
    );

    /*
      We do not show dots just when there is just one page number to be inserted between the extremes of sibling and the page limits i.e 1 and totalPageCount. Hence we are using leftSiblingIndex > 2 and rightSiblingIndex < totalPageCount - 2
    */
    const shouldShowLeftDots = leftSiblingIndex > 2;
    const shouldShowRightDots = rightSiblingIndex < totalPageCount - 2;

    const firstPageIndex = 1;
    const lastPageIndex = totalPageCount;

    /*
        Case 2: No left dots to show, but rights dots to be shown
    */
    if (!shouldShowLeftDots && shouldShowRightDots) {
        let leftItemCount = 3 + 2 * siblingCount;
        let leftRange = range(1, leftItemCount);

        return [...leftRange, DOTS2, totalPageCount];
    }

    /*
        Case 3: No right dots to show, but left dots to be shown
    */
    if (shouldShowLeftDots && !shouldShowRightDots) {

        let rightItemCount = 3 + 2 * siblingCount;
        let rightRange = range(
            totalPageCount - rightItemCount + 1,
            totalPageCount
        );
        return [firstPageIndex, DOTS1, ...rightRange];
    }

    /*
        Case 4: Both left and right dots to be shown
    */
    if (shouldShowLeftDots && shouldShowRightDots) {
        let middleRange = range(leftSiblingIndex, rightSiblingIndex);
        return [firstPageIndex, DOTS1, ...middleRange, DOTS2, lastPageIndex];
    }
}

export default function Pager({ currentPage, perPage, total, onPageChange }: { currentPage: number, perPage: number, total: number, onPageChange: Function }) {
    const siblingCount = 1;
    const totalPageCount = Math.ceil(total / perPage);
    const totalPageNumbers = siblingCount + 5;

    const rangeArr = paginationRange(currentPage, siblingCount, totalPageCount, totalPageNumbers);

    return (

        <div className="hidden sm:flex sm:flex-1 sm:items-center sm:justify-between">
            <div>
                <p className="text-sm text-gray-700">
                    Showing <span className="font-medium">{(currentPage - 1) * perPage}</span> to <span className="font-medium">{currentPage * perPage}</span> of{' '}
                    <span className="font-medium">{total}</span> results
                </p>
            </div>
            <div>
                <nav className="isolate inline-flex -space-x-px rounded-md shadow-sm" aria-label="Pagination">
                    <a
                        onClick={ () => onPageChange(currentPage - 1)}
                        className="relative inline-flex items-center rounded-l-md border border-gray-300 bg-white px-2 py-2 text-sm font-medium text-gray-500 hover:bg-gray-50 focus:z-20 cursor-pointer"
                    >
                        <span className="sr-only">Previous</span>
                        <ChevronLeftIcon className="h-5 w-5" aria-hidden="true" />
                    </a>

                    {rangeArr?.map(pageNumber => {
                        if(pageNumber === DOTS1) {
                            return (
                                <span key={'page' + DOTS1} className="relative inline-flex items-center border border-gray-300 bg-white px-4 py-2 text-sm font-medium text-gray-700">
                                    ...
                                </span>
                            )
                        } else if(pageNumber === DOTS2) {
                            return (
                                <span key={'page' + DOTS2} className="relative inline-flex items-center border border-gray-300 bg-white px-4 py-2 text-sm font-medium text-gray-700">
                                    ...
                                </span>
                            )
                        } else if(pageNumber === currentPage) {
                            return (
                                <a
                                    onClick={ () => onPageChange(pageNumber)}
                                    aria-current="page"
                                    className="relative z-10 inline-flex items-center border border-indigo-500 bg-indigo-50 px-4 py-2 text-sm font-medium text-indigo-600 focus:z-20 cursor-pointer"
                                    key={'page' + pageNumber}
                                >
                                    {pageNumber}
                                </a>
                            )
                        }
                        return (
                            <a
                                onClick={ () => onPageChange(pageNumber)}
                                aria-current="page"
                                className="relative inline-flex items-center border border-gray-300 bg-white px-4 py-2 text-sm font-medium text-gray-500 hover:bg-gray-50 focus:z-20 cursor-pointer"
                                key={'page' + pageNumber}
                            >
                                {pageNumber}
                            </a>
                        )
                    })}
                    <a
                        onClick={ () => onPageChange(currentPage + 1)}
                        className="relative inline-flex items-center rounded-r-md border border-gray-300 bg-white px-2 py-2 text-sm font-medium text-gray-500 hover:bg-gray-50 focus:z-20 cursor-pointer"
                    >
                        <span className="sr-only">Next</span>
                        <ChevronRightIcon className="h-5 w-5" aria-hidden="true" />
                    </a>
                </nav>
            </div>
        </div>
    )
}