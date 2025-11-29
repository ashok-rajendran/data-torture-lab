// Add smooth scroll for navigation links
document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function(e) {
            e.preventDefault();
            const target = document.querySelector(this.getAttribute('href'));
            if (target) {
                target.scrollIntoView({
                    behavior: 'smooth',
                    block: 'start'
                });
            }
        });
    });
});

// Dynamic years of experience calculation
document.addEventListener("DOMContentLoaded", function () {
    const joiningDate = new Date(2017, 8, 1); // September 1, 2017 (months are 0-indexed)
    const now = new Date();
    let years = now.getFullYear() - joiningDate.getFullYear();
    const m = now.getMonth() - joiningDate.getMonth();
    if (m < 0 || (m === 0 && now.getDate() < joiningDate.getDate())) {
        years--;
    }
    const yearsElem = document.getElementById('years-experience');
    if (yearsElem) {
        yearsElem.textContent = years;
    }

    // Scroll to top button logic
    const scrollBtn = document.getElementById('scrollToTopBtn');
    window.onscroll = function() {
        if (document.body.scrollTop > 200 || document.documentElement.scrollTop > 200) {
            scrollBtn.style.display = "block";
        } else {
            scrollBtn.style.display = "none";
        }
    };
    scrollBtn.onclick = function() {
        window.scrollTo({top: 0, behavior: 'smooth'});
    };
});
